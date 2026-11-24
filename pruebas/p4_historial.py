"""P4 - Historial: micro-lotes, publicacion atomica y deduplicacion.

Se ejecuta DENTRO del contenedor del historial (usa solo HTTP local + pyarrow):
  docker exec -i proyecto2-historial python - < pruebas/p4_historial.py

Recomendado: levantar el servicio con LAGO_DIR=/datalake/eventos_p4 para no
mezclar eventos de prueba con el datalake principal.
"""
import glob
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

import pyarrow.parquet as pq

BASE = os.environ.get("HISTORIAL_URL", "http://localhost:8003")
LAGO = os.environ.get("LAGO_DIR", "/datalake/eventos")
ESPERA_AUTO = float(os.environ.get("FLUSH_SEGUNDOS", "5")) + 4
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


def referencias():
    """Proyecto y anotador reales y lo que debe resultar del enriquecimiento."""
    from pymongo import MongoClient
    db = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=3000)["plataforma"]
    p = db.proyectos.find_one({"estado": "activo"})
    a = db.anotadores.find_one({"rol": "anotador", "estado": "activo"})
    r = db.anotadores.find_one({"rol": "revisor", "estado": "activo"})
    return (p["_id"], a["_id"], r["_id"],
            {"cliente_id": p["cliente_id"], "sla_seg": int(p["sla_horas"]) * 3600,
             "equipo_id": a.get("equipo_id")})


def evento(proy, anot, rev, estado="aprobada", ts="2026-11-23T10:00:00+00:00", **extra):
    e = {"evento_id": str(uuid.uuid4()), "timestamp": ts, "version": 4,
         "tarea_id": "p4-" + uuid.uuid4().hex[:8], "proyecto_id": proy, "batch_id": "p4",
         "modalidad": "imagen", "tipo_tarea": "deteccion_objetos",
         "estado_anterior": "en_revision", "estado_nuevo": estado,
         "anotador_id": anot, "revisor_id": rev, "toma_ts": "2026-11-23T09:00:00+00:00"}
    e.update(extra)
    return e


def filas(ids):
    out = []
    for f in glob.glob(f"{LAGO}/mes=*/*.parquet"):          # '*' no incluye los ocultos .tmp-*
        out += [r for r in pq.read_table(f).to_pylist() if r["evento_id"] in ids]
    return out


proy, anot, rev, esperado = referencias()
print(f"Proyecto {proy}  anotador {anot}  datalake {LAGO}\n")

st, salud = http("GET", "/salud")
ok(st == 200 and salud["servicio"] == "historial", f"/salud responde ({salud.get('estado')})")

# 1. Eventos nuevos -> aceptados
lote = [evento(proy, anot, rev) for _ in range(20)]
ok(all(http("POST", "/eventos", e)[0] == 202 for e in lote), "20 eventos nuevos aceptados (HTTP 202)")

# 2. Duplicados
res = [http("POST", "/eventos", e) for e in lote]
ok(all(s == 200 and b["estado"] == "duplicado" for s, b in res), "reenvio de los 20 -> duplicado")
uno, estados = evento(proy, anot, rev), []
hilos = [threading.Thread(target=lambda: estados.append(http("POST", "/eventos", uno)[1]["estado"]))
         for _ in range(10)]
[h.start() for h in hilos]; [h.join() for h in hilos]
ok(estados.count("aceptado") == 1, f"10 envios simultaneos del mismo evento -> {estados.count('aceptado')} aceptado")

# 3. Publicacion
ids = {e["evento_id"] for e in lote} | {uno["evento_id"]}
http("POST", "/flush")
f = filas(ids)
ok(len(f) == 21 and len({x["evento_id"] for x in f}) == 21, f"21 eventos en Parquet, cada uno una sola vez ({len(f)} filas)")
ok(all(x["cliente_id"] == esperado["cliente_id"] and x["sla_seg"] == esperado["sla_seg"]
       and x["equipo_id"] == esperado["equipo_id"] for x in f), "enriquecimiento correcto (cliente_id, sla_seg, equipo_id)")
ok(not glob.glob(f"{LAGO}/mes=*/.tmp-*"), "no quedan archivos temporales (publicacion atomica)")

# 4. Particion por mes y rechazo
viejo = evento(proy, anot, rev, ts="2026-01-15T08:00:00+00:00")
rech = evento(proy, anot, rev, "rechazada", codigo_motivo="ETIQUETA_INCORRECTA", toma_ts=None)
http("POST", "/eventos", viejo); http("POST", "/eventos", rech); http("POST", "/flush")
g = filas({viejo["evento_id"]})
ok(len(g) == 1 and f"mes=2026-01" in glob.glob(f"{LAGO}/mes=2026-01/*.parquet")[0], "evento de enero en mes=2026-01")
h = filas({rech["evento_id"]})
ok(len(h) == 1 and h[0]["estado_nuevo"] == "rechazada" and h[0]["codigo_motivo"] == "ETIQUETA_INCORRECTA",
   "el rechazo queda como 'rechazada' con su motivo")

# 5. Micro-lote automatico
auto = [evento(proy, anot, rev) for _ in range(3)]
[http("POST", "/eventos", e) for e in auto]
t0 = time.time()
while time.time() - t0 < ESPERA_AUTO and len(filas({e["evento_id"] for e in auto})) < 3:
    time.sleep(0.5)
n = len(filas({e["evento_id"] for e in auto}))
ok(n == 3, f"micro-lote publicado solo en {time.time() - t0:.1f} s (sin /flush)")

# 6. Validacion
ok(http("POST", "/eventos", {"estado_nuevo": "foo"})[0] == 422, "evento invalido -> 422")
ok(http("POST", "/eventos", evento(proy, anot, rev, estado_nuevo="inventado"))[0] == 422, "estado desconocido -> 422")

print("\nRESULTADO:", "P4 SUPERADA" if not fallos else f"{len(fallos)} FALLAS")
sys.exit(1 if fallos else 0)