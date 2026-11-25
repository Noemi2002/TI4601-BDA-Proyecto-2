"""P4 - Compactacion del datalake (archivos pequenos y duplicados).

Se ejecuta DENTRO de un contenedor del historial con lago aparte:

  docker compose -f docker-compose.spark.yml run -d --no-deps --name hist-p4 \
    -e LAGO_DIR=/datalake/eventos_p4 -e FLUSH_SEGUNDOS=60 historial
  docker exec -e LAGO_DIR=/datalake/eventos_p4 -i hist-p4 python - \
    < pruebas/p4_compactacion.py 2>&1 | tee evidence/p4-compactacion.txt

Escenario: 30 micro-lotes de 5 eventos (30 archivos pequenos) mas una copia
duplicada de uno de ellos. Tras compactar debe quedar 1 archivo, 150 filas
distintas, sin temporales, y el Historial debe seguir deduplicando.
"""
import glob
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

import pyarrow.parquet as pq

BASE = "http://localhost:8003"
LAGO = os.environ.get("LAGO_DIR", "/datalake/eventos_p4")
fallos = []


def ok(cond, msg):
    print(("OK:    " if cond else "FALLA: ") + msg)
    if not cond:
        fallos.append(msg)


def http(metodo, ruta, cuerpo=None):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(BASE + ruta, data=datos, method=metodo,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {}


def archivos():
    return sorted(glob.glob(f"{LAGO}/mes=*/*.parquet"))


def leer_todo():
    """Lee archivo por archivo (como hace un lector sin compactar). Devuelve (ids, segundos)."""
    t0 = time.perf_counter()
    ids = []
    for f in archivos():
        ids += pq.read_table(f)["evento_id"].to_pylist()
    return ids, time.perf_counter() - t0


from pymongo import MongoClient  # noqa: E402
db = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=4000)["plataforma"]
proy = db.proyectos.find_one({"estado": "activo"})["_id"]
anot = db.anotadores.find_one({"rol": "anotador", "estado": "activo"})["_id"]
rev = db.anotadores.find_one({"rol": "revisor", "estado": "activo"})["_id"]

# 1. 30 micro-lotes de 5 eventos -> 30 archivos pequenos
enviados = []
for lote in range(30):
    for _ in range(5):
        e = {"evento_id": str(uuid.uuid4()), "timestamp": "2026-11-23T10:00:00+00:00", "version": 4,
             "tarea_id": "compac-" + uuid.uuid4().hex[:8], "proyecto_id": proy, "batch_id": "compac",
             "modalidad": "imagen", "tipo_tarea": "deteccion_objetos",
             "estado_anterior": "en_revision", "estado_nuevo": "aprobada",
             "anotador_id": anot, "revisor_id": rev, "toma_ts": "2026-11-23T09:00:00+00:00"}
        enviados.append(e)
        http("POST", "/eventos", e)
    http("POST", "/flush")
ok(len(archivos()) == 30, f"{len(archivos())} archivos pequenos tras 30 micro-lotes")

# 2. Un archivo duplicado (simula una publicacion repetida)
shutil.copy(archivos()[0], os.path.dirname(archivos()[0]) + "/evt-copia-duplicada.parquet")
ids_antes, t_antes = leer_todo()
print(f"Antes: {len(archivos())} archivos, {len(ids_antes)} filas, {len(set(ids_antes))} distintas, "
      f"lectura {t_antes * 1000:.0f} ms")

# 3. Compactar
r = subprocess.run([sys.executable, "/app/compactar.py", "--lago", LAGO], capture_output=True, text=True)
print(r.stdout.strip())
ok(r.returncode == 0, "compactar.py termino sin errores")

# 4. Verificar
ids_despues, t_despues = leer_todo()
print(f"Despues: {len(archivos())} archivos, {len(ids_despues)} filas, {len(set(ids_despues))} distintas, "
      f"lectura {t_despues * 1000:.0f} ms")
ok(len(archivos()) == 1, "queda 1 solo archivo en la particion")
ok(len(ids_despues) == 150 and len(set(ids_despues)) == 150, "150 filas, cada evento una sola vez (se elimino el duplicado)")
ok(set(ids_despues) == {e["evento_id"] for e in enviados}, "no se perdio ningun evento")
ok(not glob.glob(f"{LAGO}/mes=*/.tmp-*"), "no quedan archivos temporales")
ok(http("POST", "/eventos", enviados[0])[1].get("estado") == "duplicado", "el Historial sigue deduplicando")
nuevo = dict(enviados[0], evento_id=str(uuid.uuid4()))
http("POST", "/eventos", nuevo); http("POST", "/flush")
ok(len(archivos()) == 2, "un evento nuevo despues de compactar genera un archivo evt-* aparte")

print("\nRESULTADO:", "COMPACTACION SUPERADA" if not fallos else f"{len(fallos)} FALLAS")
sys.exit(1 if fallos else 0)