"""Entrega durable, paso 4: con el Historial de vuelta, verifica la entrega.

Corre dentro del contenedor del historial (tiene pymongo y pyarrow) con TAREA_ID.
Espera hasta que el outbox se vacie y comprueba que los 4 eventos aparecen en
Parquet exactamente una vez y en el orden de las transiciones.
"""
import glob
import os
import sys
import time

import pyarrow.parquet as pq
from pymongo import MongoClient

LAGO = os.environ.get("LAGO_DIR", "/datalake/eventos")
TID = os.environ["TAREA_ID"]
ESPERA_MAX = 120
db = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=4000)["plataforma"]
fallos = []


def ok(cond, msg):
    print(("OK:    " if cond else "FALLA: ") + msg)
    if not cond:
        fallos.append(msg)


def filas():
    out = []
    for f in glob.glob(f"{LAGO}/mes=*/*.parquet"):
        out += pq.read_table(f, filters=[("tarea_id", "=", TID)]).to_pylist()
    return sorted(out, key=lambda r: r["version"])


t0 = time.time()
while time.time() - t0 < ESPERA_MAX:
    if db.outbox_eventos.count_documents({"evento.tarea_id": TID}) == 0 and len(filas()) >= 4:
        break
    time.sleep(1)
segundos = time.time() - t0
f = filas()
estados = [r["estado_nuevo"] for r in f]
print(f"Parquet: {estados} ({segundos:.1f} s despues de reiniciar el Historial)")
ok(db.outbox_eventos.count_documents({"evento.tarea_id": TID}) == 0, "el outbox quedo vacio (todo fue entregado)")
ok(estados == ["abierta", "anotada", "en_revision", "rechazada"], "Parquet tiene los 4 eventos en orden")
ok(len({r["evento_id"] for r in f}) == len(f), "cada evento aparece una sola vez")
ok(f and f[-1]["codigo_motivo"] == "ETIQUETA_INCORRECTA", "la fila rechazada conserva su codigo de motivo")
ok(db.tareas.find_one({"_id": TID})["estado"] == "disponible", "MongoDB: la tarea sigue 'disponible'")
print(f"Segundos desde que el Historial volvio hasta la entrega completa: {segundos:.1f}")
print("\nRESULTADO:", "ENTREGA DURABLE SUPERADA" if not fallos else f"{len(fallos)} FALLAS")
sys.exit(1 if fallos else 0)