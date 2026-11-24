#!/usr/bin/env bash
# Caida del Historial con eventos aun sin publicar: el WAL debe recuperarlos.
# Uso (desde la raiz del repo):
#   bash pruebas/p4_reinicio_buffer.sh 2>&1 | tee evidence/p4-reinicio-historial.txt
set -u
COMPOSE="docker compose -f docker-compose.spark.yml"
echo "Fecha: $(date -u +%FT%TZ)"
echo "Objetivo: matar el Historial con eventos en buffer y comprobar que el WAL los recupera una sola vez."

docker rm -f hist-p4 >/dev/null 2>&1
sudo rm -rf datalake/eventos_p4 datalake/eventos_p4_wal datalake/eventos_p4_ids.json
# FLUSH_SEGUNDOS largo: los eventos se quedan en buffer el tiempo suficiente para matar el contenedor
$COMPOSE run -d --no-deps --name hist-p4 -e LAGO_DIR=/datalake/eventos_p4 -e FLUSH_SEGUNDOS=20 historial >/dev/null
sleep 6

echo; echo "== 1. Enviar 20 eventos (quedan en buffer + WAL)"
docker exec -e MODO=enviar -i hist-p4 python - < pruebas/p4_reinicio_buffer.py
ENVIO=$?

echo; echo "== 2. docker kill (caida abrupta, sin cierre ordenado)"
docker kill hist-p4 >/dev/null; echo "contenedor detenido a las $(date -u +%T)Z"

echo; echo "== 3. docker start (recuperacion)"
docker start hist-p4 >/dev/null
for i in $(seq 1 30); do
  docker exec hist-p4 python -c "import urllib.request;urllib.request.urlopen('http://localhost:8003/salud')" 2>/dev/null && break
  sleep 1
done
echo "servicio de nuevo en linea tras ~${i} s"
docker logs hist-p4 2>&1 | grep -E "WAL|Deduplicacion" | tail -3

echo; echo "== 4. Verificar"
docker exec -e MODO=verificar -i hist-p4 python - < pruebas/p4_reinicio_buffer.py
VERIF=$?

echo; echo "== Limpieza"
docker rm -f hist-p4 >/dev/null
sudo rm -rf datalake/eventos_p4 datalake/eventos_p4_wal datalake/eventos_p4_ids.json
exit $(( ENVIO + VERIF ))