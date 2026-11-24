"""P4/P8 - Historial: caida con eventos aun en el buffer (sin publicar en Parquet).

Lo orquesta pruebas/p4_reinicio_buffer.sh (docker kill / docker start).
Este archivo corre DENTRO del contenedor hist-p4 con la variable MODO:

  MODO=enviar     manda 20 eventos nuevos (HTTP 202) y verifica que AUN no estan en Parquet
                  (siguen en el buffer / WAL). Guarda sus ids en <lago>_ids.json.
  MODO=verificar  tras el reinicio: espera a que se publiquen, comprueba que cada uno
                  aparece EXACTAMENTE una vez, y que reenviarlos devuelve 'duplicado'.
"""
import glob
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

import pyarrow.parquet as pq

BASE = "http://localhost:8003"
LAGO = os.environ.get("LAGO_DIR", "/datalake/eventos_p4")
IDS = LAGO + "_ids.json"
MODO = os.environ.get("MODO", "")
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


def filas(ids):
    out = []
    for f in glob.glob(f"{LAGO}/mes=*/*.parquet"):
        out += [r for r in pq.read_table(f).to_pylist() if r["evento_id"] in ids]
    return out


if MODO == "enviar":
    from pymongo import MongoClient
    db = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=4000)["plataforma"]
    p = db.proyectos.find_one({"estado": "activo"})
    a = db.anotadores.find_one({"rol": "anotador", "estado": "activo"})
    r = db.anotadores.find_one({"rol": "revisor", "estado": "activo"})
    lote = [{"evento_id": str(uuid.uuid4()), "timestamp": "2026-11-23T10:00:00+00:00",
             "version": 4, "tarea_id": "reinicio-" + uuid.uuid4().hex[:8], "proyecto_id": p["_id"],
             "batch_id": "reinicio", "modalidad": "imagen", "tipo_tarea": "deteccion_objetos",
             "estado_anterior": "en_revision", "estado_nuevo": "aprobada",
             "anotador_id": a["_id"], "revisor_id": r["_id"],
             "toma_ts": "2026-11-23T09:00:00+00:00"} for _ in range(20)]
    ok(all(http("POST", "/eventos", e)[0] == 202 for e in lote), "20 eventos aceptados (HTTP 202)")
    ids = [e["evento_id"] for e in lote]
    json.dump({"ids": ids, "eventos": lote}, open(IDS, "w"))
    st, salud = http("GET", "/salud")
    ok(salud["pendientes_en_buffer"] == 20 and len(filas(set(ids))) == 0,
       f"antes de la caida: {salud['pendientes_en_buffer']} en buffer y 0 en Parquet")

elif MODO == "verificar":
    guardado = json.load(open(IDS))
    ids, lote = set(guardado["ids"]), guardado["eventos"]
    st, salud = http("GET", "/salud")
    print(f"Tras el reinicio: {salud}")
    t0 = time.time()
    while time.time() - t0 < 40 and len(filas(ids)) < 20:
        time.sleep(0.5)
    f = filas(ids)
    ok(len(f) == 20 and len({x["evento_id"] for x in f}) == 20,
       f"los 20 eventos recuperados del WAL estan en Parquet, cada uno una vez ({len(f)} filas, "
       f"{time.time() - t0:.1f} s tras el reinicio)")
    res = [http("POST", "/eventos", e) for e in lote]
    ok(all(s == 200 and b.get("estado") == "duplicado" for s, b in res),
       "reenviar los 20 despues del reinicio -> 'duplicado'")
    http("POST", "/flush")
    ok(len(filas(ids)) == 20, "tras el reenvio siguen 20 filas (no se duplico nada)")
    ok(not glob.glob(f"{LAGO}/mes=*/.tmp-*"), "no quedan archivos temporales")
    print("\nRESULTADO:", "REINICIO CON BUFFER SUPERADA" if not fallos else f"{len(fallos)} FALLAS")
else:
    sys.exit("Define MODO=enviar o MODO=verificar")
sys.exit(1 if fallos else 0)