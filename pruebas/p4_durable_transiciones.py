"""Entrega durable, paso 2: genera 4 transiciones reales con el Historial CAIDO.

Lo ejecuta pruebas/p4_entrega_durable.sh dentro del contenedor `carga`
(misma red que el Registro). Hace siguiente -> enviar -> iniciar revision ->
rechazar sobre el Registro y comprueba que los 4 eventos quedan guardados en la
coleccion `outbox_eventos` (pendientes, sin perderse).
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

from pymongo import MongoClient

REGISTRO = os.environ.get("REGISTRO_URL", "http://registro:8001").rstrip("/")
MONGO = os.environ.get("MONGO_URI",
                       "mongodb://mongo-1:27017,mongo-2:27017,mongo-3:27017/?replicaSet=rs-plataforma")
db = MongoClient(MONGO, serverSelectionTimeoutMS=4000)["plataforma"]


def http(ruta, cuerpo):
    req = urllib.request.Request(REGISTRO + ruta, data=json.dumps(cuerpo).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {}


t = db.tareas.find_one({"estado": "disponible"})
proy = db.proyectos.find_one({"_id": t["proyecto_id"], "estado": "activo"})
anot = db.anotadores.find_one({"rol": "anotador", "estado": "activo"})["_id"]
rev = db.anotadores.find_one({"rol": "revisor", "estado": "activo"})["_id"]
token = str(uuid.uuid4())
etiqueta = proy["taxonomia"][0]["etiqueta"]

st, tomada = http("/tareas/siguiente", {"proyecto_id": proy["_id"], "anotador_id": anot, "token": token})
assert st == 200, f"siguiente -> {st}"
tid = tomada["tarea_id"]
geo = db.tareas.find_one({"_id": tid})["geometria"]
st, _ = http(f"/tareas/{tid}/enviar", {"anotador_id": anot, "token": token, "anotacion": {
    "geometria": geo, "etiquetas": [etiqueta], "elementos": [{"etiqueta": etiqueta, "nota": "durable"}]}})
assert st == 200, f"enviar -> {st}"
st, _ = http(f"/tareas/{tid}/revision", {"revisor_id": rev, "accion": "iniciar"})
assert st == 200, f"iniciar -> {st}"
st, r = http(f"/tareas/{tid}/revision", {"revisor_id": rev, "accion": "rechazar", "codigo_motivo": "ETIQUETA_INCORRECTA"})
assert st == 200, f"rechazar -> HTTP {st}"
print(f"Transiciones confirmadas por MongoDB (HTTP 200) con el Historial caido. Tarea {tid}")

time.sleep(2)
pend = list(db.outbox_eventos.find({"evento.tarea_id": tid}))
estados = [p["evento"]["estado_nuevo"] for p in sorted(pend, key=lambda p: p["evento"]["version"])]
print(f"Outbox: {len(pend)} eventos guardados {estados}; intentos de entrega: {[p['intentos'] for p in pend]}")
estado_mongo = db.tareas.find_one({"_id": tid})["estado"]
print(f"MongoDB: estado={estado_mongo}")
print(f"TAREA_ID={tid}")
ok = len(pend) == 4 and estados == ["abierta", "anotada", "en_revision", "rechazada"] and estado_mongo == "disponible"
print("OK:    los 4 eventos estan en el outbox, ninguno se perdio" if ok else "FALLA: el outbox no tiene los 4 eventos esperados")
sys.exit(0 if ok else 1)