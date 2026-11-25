#!/usr/bin/env bash

set -euo pipefail

# Ejecutar siempre desde la raíz del proyecto
cd "$(dirname "$0")/.."

mkdir -p evidence

ID="p8_$(date +%Y%m%d_%H%M%S)"

MARCAS="evidence/p8_marcas_${ID}.jsonl"
DETENER="evidence/p8_detener_${ID}.flag"

PID=""


# ------------------------------------------
# Registrar eventos del experimento
# ------------------------------------------

marcar() {
    printf '{"evento":"%s","ts":%s}\n' \
        "$1" "$(date +%s.%N)" >> "$MARCAS"

    echo "EVENTO: $1"
}


# ------------------------------------------
# Detectar el PRIMARY actual
# ------------------------------------------

primario() {
    local servicio estado

    for servicio in mongo-1 mongo-2 mongo-3; do

        estado=$(
            docker compose exec -T "$servicio" \
                mongosh --quiet --eval \
                'print(
                    db.hello().isWritablePrimary
                        ? "PRIMARY"
                        : "OTHER"
                )' 2>/dev/null || true
        )

        if [[ "$estado" == "PRIMARY" ]]; then
            echo "$servicio"
            return 0
        fi
    done

    return 1
}


# ------------------------------------------
# Esperar que estén saludables los 3 nodos
# ------------------------------------------

esperar_tres() {
    local estados

    for ((i = 0; i < 60; i++)); do

        estados=$(
            docker compose exec -T mongo-1 \
                mongosh --quiet --eval \
                'print(
                    rs.status().members
                        .filter(m => m.health === 1)
                        .map(m => m.stateStr)
                        .sort()
                        .join(",")
                )' 2>/dev/null || true
        )

        if [[ "$estados" == \
              "PRIMARY,SECONDARY,SECONDARY" ]]; then
            return 0
        fi

        sleep 2
    done

    echo "ERROR: no recuperaron los tres miembros" >&2
    return 1
}


# ------------------------------------------
# Esperar elección de un PRIMARY
# ------------------------------------------

esperar_primario() {

    for ((i = 0; i < 40; i++)); do

        if primario >/dev/null; then
            return 0
        fi

        sleep 2
    done

    echo "ERROR: no se eligió un nuevo PRIMARY" >&2
    return 1
}


# ------------------------------------------
# Restauración incluso si ocurre un error
# ------------------------------------------

restaurar() {
    local codigo=$?

    trap - EXIT

    # Señal para detener la sonda
    touch "$DETENER"

    # Intentar recuperar los tres nodos
    docker compose up -d \
        mongo-1 mongo-2 mongo-3 \
        >/dev/null 2>&1 || true

    if [[ -n "$PID" ]]; then
        wait "$PID" || true
    fi

    if [[ $codigo -ne 0 ]]; then
        echo "Hubo un error durante P8." >&2
        echo "Se intentó restaurar MongoDB." >&2
        echo "Verifica con docker compose ps." >&2
    fi

    exit "$codigo"
}

trap restaurar EXIT


# ------------------------------------------
# Preparación inicial
# ------------------------------------------

echo "ID de la ejecución: $ID"

printf '%s\n' "$ID" > \
    evidence/p8_ultimo_id.txt

docker compose up -d \
    mongo-1 mongo-2 mongo-3 registro

esperar_tres

echo "OK: replica set inicial saludable"


# Guardar estado inicial
docker compose exec -T mongo-1 \
    mongosh --quiet --eval \
    'printjson(
        rs.status().members.map(m => ({
            name: m.name,
            state: m.stateStr,
            health: m.health
        }))
    )' > "evidence/p8_estado_inicial_${ID}.txt"


# ------------------------------------------
# Iniciar sonda continua de escrituras
# ------------------------------------------

echo "Iniciando sonda de escrituras..."

docker compose run -T --rm --no-deps carga \
    python pruebas/p8_medir.py \
    sonda --id "$ID" \
    > "evidence/p8_sonda_proceso_${ID}.log" 2>&1 &

PID=$!

SONDA="evidence/p8_sonda_${ID}.jsonl"

# Esperar una escritura confirmada antes
# de comenzar a provocar fallas
for ((i = 0; i < 30; i++)); do

    if [[ -s "$SONDA" ]] &&
       grep -q '"estado": "ACK"' "$SONDA"; then
        break
    fi

    sleep 1
done

if [[ ! -s "$SONDA" ]] ||
   ! grep -q '"estado": "ACK"' "$SONDA"; then

    echo "ERROR: la sonda no confirmó escrituras." >&2
    echo "Revisa el archivo de log de la sonda." >&2
    exit 1
fi

echo "OK: sonda registrando escrituras"


# ==========================================
# FASE A: CAÍDA DE UN SECUNDARIO
# ==========================================

echo ""
echo "FASE A: Caída de un secundario"

ORIGINAL=$(primario)
SECUNDARIO=""

for s in mongo-1 mongo-2 mongo-3; do

    if [[ "$s" != "$ORIGINAL" ]]; then
        SECUNDARIO="$s"
        break
    fi
done

echo "Primario actual: $ORIGINAL"
echo "Secundario que se detendrá: $SECUNDARIO"

marcar secundario_inicio

docker compose stop -t 2 "$SECUNDARIO"

# Mantener la sonda mientras hay dos nodos
sleep 15

marcar secundario_fin

# Restaurar el secundario
docker compose up -d "$SECUNDARIO"

esperar_tres

sleep 4

echo "OK: secundario restaurado"


# ==========================================
# FASE B: CAÍDA DEL PRIMARIO
# ==========================================

echo ""
echo "FASE B: Caída del primario"

# Detectar el primario nuevamente:
# no asumir que sigue siendo el mismo
ORIGINAL=$(primario)

echo "Primario que se detendrá: $ORIGINAL"

marcar primario_detener_inicio

docker compose stop -t 2 "$ORIGINAL"

marcar primario_caido

# Esperar una nueva elección
esperar_primario

NUEVO=$(primario)

echo "Primario anterior: $ORIGINAL"
echo "Nuevo primario: $NUEVO"

if [[ "$NUEVO" == "$ORIGINAL" ]]; then

    echo "ERROR: no ocurrió cambio de PRIMARY" >&2
    exit 1
fi

# Continuar escrituras con el nuevo primario
sleep 8

# Restaurar el nodo anterior
docker compose up -d "$ORIGINAL"

esperar_tres

sleep 4

echo "OK: elección y restauración completadas"


# ==========================================
# FASE C: PÉRDIDA DE MAYORÍA
# ==========================================

echo ""
echo "FASE C: Pérdida de mayoría"

ACTUAL=$(primario)

SECUNDARIOS=()

for s in mongo-1 mongo-2 mongo-3; do

    if [[ "$s" != "$ACTUAL" ]]; then
        SECUNDARIOS+=("$s")
    fi
done

echo "Nodo que quedará encendido: $ACTUAL"
echo "Nodos que se detendrán: ${SECUNDARIOS[*]}"

# Dejar solamente un nodo disponible
docker compose stop -t 2 \
    "${SECUNDARIOS[@]}"

marcar sin_mayoria_inicio

# Se esperan errores de confirmación
# porque no existe una mayoría de 2 nodos
sleep 20

marcar sin_mayoria_fin

# Recuperar los nodos detenidos
docker compose up -d \
    "${SECUNDARIOS[@]}"

esperar_tres

sleep 5

echo "OK: mayoría restaurada"


# ==========================================
# TERMINAR LA SONDA
# ==========================================

echo ""
echo "Deteniendo sonda..."

touch "$DETENER"

wait "$PID"

PID=""

echo "OK: sonda finalizada"


# ==========================================
# VERIFICAR RPO Y RTO OBSERVADO
# ==========================================

echo ""
echo "Verificando escrituras confirmadas..."

docker compose run -T --rm --no-deps carga \
    python pruebas/p8_medir.py \
    verificar --id "$ID" \
    | tee "evidence/p8_reporte_${ID}.txt"


# ==========================================
# BENCHMARK: w:1 VS w:majority
# ==========================================

echo ""
echo "Ejecutando benchmark..."

docker compose run -T --rm --no-deps carga \
    python pruebas/p8_medir.py \
    benchmark \
    --id "$ID" \
    --muestras 100 \
    | tee "evidence/p8_benchmark_resumen_${ID}.txt"


# ==========================================
# EVIDENCIA FINAL
# ==========================================

docker compose exec -T mongo-1 \
    mongosh --quiet --eval \
    'printjson(
        rs.status().members.map(m => ({
            name: m.name,
            state: m.stateStr,
            health: m.health
        }))
    )' > "evidence/p8_estado_final_${ID}.txt"


echo ""
echo "======================================"
echo "OK: escenarios P8 terminados"
echo "ID: $ID"
echo "Evidencias guardadas en evidence/"
echo "======================================"