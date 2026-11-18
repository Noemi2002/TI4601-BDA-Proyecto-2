#!/usr/bin/env bash

set -u

SALIDA="$1"

CONTENEDORES=(
  proyecto2-gateway
  proyecto2-registro
  proyecto2-sesion
  proyecto2-valkey
  proyecto2-mongo-1
  proyecto2-mongo-2
  proyecto2-mongo-3
)

echo \
"timestamp,nombre,cpu,memoria,mem_pct,net_io,block_io,pids" \
> "$SALIDA"

while true
do
    TIMESTAMP=$(date --iso-8601=seconds)

    docker stats \
      --no-stream \
      --format \
'{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}' \
      "${CONTENEDORES[@]}" |
    while IFS= read -r LINEA
    do
        echo "$TIMESTAMP,$LINEA"
    done >> "$SALIDA"

    sleep 2
done