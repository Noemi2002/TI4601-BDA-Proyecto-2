#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "Uso: $0 <usuarios> <corrida>"
    exit 1
fi

USUARIOS="$1"
CORRIDA="$2"

PROYECTO_ID="b3f242ed-755a-5b7a-9a41-ab68d3aa50fd"

ID="p6_u${USUARIOS}_r${CORRIDA}"

RESULTADO="evidence/${ID}.json"
LOG="evidence/${ID}.log"
STATS="evidence/${ID}_docker_stats.csv"

echo "======================================"
echo " P6"
echo " Usuarios: $USUARIOS"
echo " Corrida:  $CORRIDA"
echo "======================================"

echo "Iniciando captura de docker stats..."

bash pruebas/p6_docker_stats.sh "$STATS" &
STATS_PID=$!

limpiar() {
    kill "$STATS_PID" 2>/dev/null || true
    wait "$STATS_PID" 2>/dev/null || true
}

trap limpiar EXIT INT TERM

echo "Ejecutando carga..."

docker compose --profile pruebas run --rm simulador \
  carga.py \
  --proyecto-id "$PROYECTO_ID" \
  --concurrencia "$USUARIOS" \
  --warmup 30 \
  --duracion 90 \
  --intervalo 2 \
  > "$RESULTADO" \
  2> "$LOG"

limpiar
trap - EXIT INT TERM

echo
echo "Resultado:"
cat "$RESULTADO"

echo
echo "Archivos:"
echo "  $RESULTADO"
echo "  $LOG"
echo "  $STATS"