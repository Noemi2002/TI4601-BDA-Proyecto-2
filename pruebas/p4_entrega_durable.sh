#!/usr/bin/env bash
# Entrega durable de eventos: el Historial cae, el Registro sigue confirmando
# transiciones (HTTP 200) y despues se reinicia el propio Registro. Al volver el
# Historial, todos los eventos deben llegar a Parquet exactamente una vez.
#
# Uso (desde la raiz del repo, con el compose principal y el de Spark arriba):
#   bash pruebas/p4_entrega_durable.sh 2>&1 | tee evidence/p4-entrega-durable.txt
set -u
SPARK="docker compose -f docker-compose.spark.yml"
echo "Fecha: $(date -u +%FT%TZ)"
echo "Objetivo: ningun evento confirmado por MongoDB se pierde aunque el Historial y el Registro se reinicien."

echo; echo "== 1. Detener el Historial"
docker stop proyecto2-historial >/dev/null && echo "historial detenido a las $(date -u +%T)Z"

echo; echo "== 2. Transiciones reales por el Registro con el Historial caido"
SALIDA=$(docker compose run --rm --no-deps -T carga python - < pruebas/p4_durable_transiciones.py 2>&1)
PASO2=$?
echo "$SALIDA"
TAREA_ID=$(echo "$SALIDA" | sed -n 's/^TAREA_ID=//p' | tail -1)
[ -z "$TAREA_ID" ] && { echo "FALLA: no se obtuvo TAREA_ID"; docker start proyecto2-historial >/dev/null; exit 1; }

echo; echo "== 3. Reiniciar el Registro (los eventos pendientes viven en MongoDB, no en memoria)"
docker restart proyecto2-registro >/dev/null
for i in $(seq 1 30); do
  docker exec proyecto2-registro python -c "import urllib.request;urllib.request.urlopen('http://localhost:8001/salud')" 2>/dev/null && break
  sleep 1
done
echo "registro de nuevo en linea tras ~${i} s"

echo; echo "== 4. Levantar el Historial y verificar la entrega"
docker start proyecto2-historial >/dev/null
for i in $(seq 1 30); do
  docker exec proyecto2-historial python -c "import urllib.request;urllib.request.urlopen('http://localhost:8003/salud')" 2>/dev/null && break
  sleep 1
done
echo "historial de nuevo en linea tras ~${i} s ($(date -u +%T)Z)"
docker exec -e TAREA_ID="$TAREA_ID" -i proyecto2-historial python - < pruebas/p4_durable_verificar.py
PASO4=$?
exit $(( PASO2 + PASO4 ))