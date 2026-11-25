"""Reconciliacion MongoDB <-> datalake: detecta eventos de transicion que faltan.

Cada transicion guarda `ultimo_evento_id` en la tarea. Este script busca las
tareas cuyo ultimo evento no esta ni en el datalake ni pendiente en el outbox.
Solo puede detectar el ULTIMO evento de cada tarea (no los intermedios) y solo
las que cambiaron desde la fecha indicada (los datos anteriores a la integracion
del Historial no tienen eventos y apareceran como faltantes).

Se ejecuta dentro del contenedor del historial:
  docker exec -e DESDE=2026-11-24T00:00:00+00:00 -i proyecto2-historial python - \
    < pruebas/reconciliar_eventos.py

Variables: DESDE (ISO 8601, obligatoria), LAGO_DIR (por defecto /datalake/eventos).
"""
import glob
import json
import os
import sys
from datetime import datetime

import pyarrow.parquet as pq
from pymongo import MongoClient

LAGO = os.environ.get("LAGO_DIR", "/datalake/eventos")
if "DESDE" not in os.environ:
    sys.exit("Defina DESDE (ISO 8601), p. ej. DESDE=2026-11-24T00:00:00+00:00")
desde = datetime.fromisoformat(os.environ["DESDE"])
db = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=4000)["plataforma"]

en_lago = set()
for f in glob.glob(f"{LAGO}/mes=*/*.parquet"):
    en_lago.update(pq.read_table(f, columns=["evento_id"])["evento_id"].to_pylist())
en_outbox = {d["_id"] for d in db.outbox_eventos.find({}, {"_id": 1})}

tareas = list(db.tareas.find({"actualizado_en": {"$gte": desde}, "ultimo_evento_id": {"$ne": None}},
                             {"_id": 1, "estado": 1, "ultimo_evento_id": 1}))
faltan = [t for t in tareas if t["ultimo_evento_id"] not in en_lago and t["ultimo_evento_id"] not in en_outbox]
pendientes = [t for t in tareas if t["ultimo_evento_id"] in en_outbox and t["ultimo_evento_id"] not in en_lago]

print(f"Tareas modificadas desde {desde.isoformat()}: {len(tareas)}")
print(f"  ultimo evento ya en el datalake:      {len(tareas) - len(faltan) - len(pendientes)}")
print(f"  ultimo evento pendiente en el outbox: {len(pendientes)}")
print(f"  ultimo evento FALTANTE:               {len(faltan)}")
for t in faltan[:10]:
    print(f"    tarea {t['_id']} estado={t['estado']} evento_id={t['ultimo_evento_id']}")
print("RESUMEN_RECONCILIACION_JSON", json.dumps(
    {"desde": desde.isoformat(), "tareas": len(tareas), "pendientes_outbox": len(pendientes),
     "faltantes": len(faltan)}))
sys.exit(1 if faltan else 0)