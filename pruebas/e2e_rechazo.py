"""Prueba de extremo a extremo: recorrido de una tarea con RECHAZO real.

Flujo: Registro (siguiente -> enviar -> 2.o enviar -> iniciar revision -> rechazar)
y verificacion de lo que queda en MongoDB y en el datalake Parquet.

Comprueba:
  - 2.o envio de la misma tarea -> HTTP 409 y no escribe fila en el datalake.
  - En MongoDB la tarea rechazada queda 'disponible' (asignacion vacia, version +4).
  - En Parquet hay 4 filas de esa tarea (abierta, anotada, en_revision, rechazada)
    y la ultima es 'rechazada' con su codigo de motivo; ninguna fila 'disponible'.
  - Segundos entre el HTTP 200 del rechazo y la fila visible en Parquet.

Se ejecuta DENTRO del contenedor del historial (ya esta en la red, tiene
MONGO_URI, pymongo y pyarrow):

  docker exec -i proyecto2-historial python - < pruebas/e2e_rechazo.py \
      2>&1 | tee evidence/e2e-rechazo.txt

Variables opcionales:
  REGISTRO_URL  (por defecto http://registro:8001)
  LAGO_DIR      (por defecto /datalake/eventos)
  SENAL         ruta de un archivo donde se anota {evento_id, t0} del rechazo, para
                que un job de Spark lo detecte (ver spark/jobs/esperar_evento.py)

Efecto sobre los datos: consume una tarea 'disponible' y la deja de nuevo
'disponible' (version +4) y agrega 4 eventos reales al historial.
"""
import glob
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

import pyarrow.parquet as pq
from pymongo import MongoClient

REGISTRO = os.environ.get("REGISTRO_URL", "http://registro:8001").rstrip("/")
LAGO = os.environ.get("LAGO_DIR", "/datalake/eventos")
SENAL = os.environ.get("SENAL")
ESPERA_MAX = 60
fallos = []


def ok(cond, msg):
    print(("OK:    " if cond else "FALLA: ") + msg)
    if not cond:
        fallos.append(msg)


def http(ruta, cuerpo):
    req = urllib.request.Request(
        REGISTRO + ruta, data=json.dumps(cuerpo).encode(), method="POST",
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, {"detalle": raw.decode(errors="replace")}


def filas_parquet(tarea_id):
    out = []
    for f in glob.glob(f"{LAGO}/mes=*/*.parquet"):      # '*' no incluye ocultos .tmp-*
        out += pq.read_table(f, filters=[("tarea_id", "=", tarea_id)]).to_pylist()
    return sorted(out, key=lambda r: r["version"])


db = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=4000)["plataforma"]

# --- Datos reales de partida -------------------------------------------------
t_disp = db.tareas.find_one({"estado": "disponible"})
proy = db.proyectos.find_one({"_id": t_disp["proyecto_id"], "estado": "activo"}) if t_disp else None
if proy is None:
    sys.exit("No hay tareas 'disponible' de un proyecto activo en MongoDB.")
anot = db.anotadores.find_one({"rol": "anotador", "estado": "activo"})
rev = db.anotadores.find_one({"rol": "revisor", "estado": "activo"})
etiqueta = proy["taxonomia"][0]["etiqueta"]
token = str(uuid.uuid4())
print(f"Registro {REGISTRO}  datalake {LAGO}")
print(f"Proyecto {proy['_id']}  anotador {anot['_id']}  revisor {rev['_id']}\n")

# --- 1. Tomar la tarea -------------------------------------------------------
st, tomada = http("/tareas/siguiente",
                  {"proyecto_id": proy["_id"], "anotador_id": anot["_id"], "token": token})
ok(st == 200, f"POST /tareas/siguiente -> {st}")
if st != 200:
    sys.exit(f"No se pudo tomar una tarea: {tomada}")
tid = tomada["tarea_id"]
doc = db.tareas.find_one({"_id": tid})
v0 = doc["version"] - 1                                   # version antes de la toma
print(f"Tarea {tid}  modalidad {tomada['modalidad']}  version inicial {v0}")
ok(doc["estado"] == "abierta", "MongoDB: estado 'abierta' tras la toma")

# --- 2. Enviar y reenviar ----------------------------------------------------
envio = {"anotador_id": anot["_id"], "token": token,
         "anotacion": {"geometria": doc["geometria"], "etiquetas": [etiqueta],
                       "elementos": [{"etiqueta": etiqueta, "nota": "e2e-rechazo"}]}}
st, _ = http(f"/tareas/{tid}/enviar", envio)
ok(st == 200, f"POST enviar -> {st}")
st2, cuerpo2 = http(f"/tareas/{tid}/enviar", envio)
ok(st2 == 409, f"segundo envio de la misma tarea -> {st2} (se espera 409)")
ok(db.tareas.find_one({"_id": tid})["estado"] == "anotada", "MongoDB: sigue 'anotada' tras el 409")

# --- 3. Revision: iniciar y rechazar ----------------------------------------
st, r_ini = http(f"/tareas/{tid}/revision", {"revisor_id": rev["_id"], "accion": "iniciar"})
ok(st == 200 and r_ini.get("estado") == "en_revision", f"iniciar revision -> {st} {r_ini.get('estado')}")
st, r_rech = http(f"/tareas/{tid}/revision",
                  {"revisor_id": rev["_id"], "accion": "rechazar",
                   "codigo_motivo": "ETIQUETA_INCORRECTA"})
t_http200 = time.time()
ok(st == 200, f"rechazar -> HTTP {st}")
if SENAL:
    with open(SENAL, "w") as f:
        json.dump({"evento_id": r_rech.get("evento_id"), "t0": t_http200}, f)

# --- 4. Lo que quedo en MongoDB ---------------------------------------------
final = db.tareas.find_one({"_id": tid})
print(f"\nMongoDB -> estado={final['estado']} version={final['version']} asignacion={final.get('asignacion')}")
ok(final["estado"] == "disponible", "MongoDB: la tarea rechazada vuelve a 'disponible'")
ok(final.get("asignacion") is None, "MongoDB: la asignacion queda vacia")
ok(final["version"] == v0 + 4, f"MongoDB: version {v0} -> {final['version']} (+4 transiciones)")
ok(r_rech.get("estado") == "disponible" and r_rech.get("estado_evento") == "rechazada",
   "la respuesta distingue estado Mongo 'disponible' y evento 'rechazada'")

# --- 5. Lo que llega al datalake --------------------------------------------
print("\nEsperando el micro-lote del Historial...")
filas = []
while time.time() - t_http200 < ESPERA_MAX:
    filas = filas_parquet(tid)
    if len(filas) >= 4:
        break
    time.sleep(0.5)
t_visible = time.time() - t_http200
estados = [r["estado_nuevo"] for r in filas]
print(f"Parquet -> {estados}")
ok(estados == ["abierta", "anotada", "en_revision", "rechazada"],
   "Parquet: abierta -> anotada -> en_revision -> rechazada")
ok("disponible" not in estados, "Parquet: ninguna fila 'disponible' (el rechazo queda 'rechazada')")
if filas:
    u = filas[-1]
    ok(u["codigo_motivo"] == "ETIQUETA_INCORRECTA" and u["revisor_id"] == rev["_id"],
       "Parquet: la fila 'rechazada' trae codigo_motivo y revisor")
    ok(u["cliente_id"] == proy["cliente_id"] and u["sla_seg"] == int(proy["sla_horas"]) * 3600,
       "Parquet: enriquecimiento correcto (cliente_id, sla_seg)")
    ok(len({r["evento_id"] for r in filas}) == len(filas), "Parquet: cada evento una sola vez")
    ok(u["version"] == final["version"], "Parquet: version del evento = version en MongoDB")
print(f"\nSegundos entre el HTTP 200 del rechazo y la fila visible en Parquet: {t_visible:.1f}")
print("RESUMEN_E2E_JSON", json.dumps({
    "fecha": datetime.now(timezone.utc).isoformat(), "tarea_id": tid,
    "estado_mongodb": final["estado"], "estados_parquet": estados,
    "segundos_http200_a_parquet": round(t_visible, 2)}))

print("\nRESULTADO:", "E2E RECHAZO SUPERADA" if not fallos else f"{len(fallos)} FALLAS")
sys.exit(1 if fallos else 0)