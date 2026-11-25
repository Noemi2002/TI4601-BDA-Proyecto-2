"""Cliente para entregar eventos de transicion al servicio de Historial.

Entrega DURABLE (patron "outbox"):
- Se llama DESPUES de que MongoDB confirma la transicion.
- El evento se guarda primero en la coleccion `outbox_eventos` (w:"majority"),
  con `_id = evento_id`. Asi sobrevive a una caida del Historial y tambien al
  reinicio del Registro.
- Un hilo de fondo envia los pendientes al Historial EN ORDEN y los borra solo
  cuando el Historial responde 202 (aceptado) o 200 (duplicado). Si el Historial
  no responde, reintenta sin limite con espera creciente (0.5 s hasta 30 s).
- El Historial deduplica por evento_id, por lo que los reintentos son seguros
  (entrega al menos una vez).
- Un evento rechazado como invalido (HTTP 4xx) no se reintenta: queda en la
  coleccion con estado "invalido" para revision manual.
- Nunca retrasa ni rompe la respuesta al cliente.

Ventana residual: si el proceso del Registro muere entre el commit de la
transicion en MongoDB y la insercion en el outbox, el evento no se genera.
Puede detectarse comparando `ultimo_evento_id` de las tareas con el datalake
(pruebas/reconciliar_eventos.py).
"""
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from urllib import error, request

from pymongo import WriteConcern
from pymongo.errors import PyMongoError

from database import db

logger = logging.getLogger("registro.historial")

HISTORIAL_URL = os.environ.get("HISTORIAL_URL", "http://historial:8003")
TIMEOUT_SEG = float(os.environ.get("HISTORIAL_TIMEOUT", "2"))
ACTIVO = os.environ.get("HISTORIAL_ACTIVO", "true").lower() != "false"
ESPERA_MIN = 0.5
ESPERA_MAX = float(os.environ.get("HISTORIAL_ESPERA_MAX", "30"))
SONDEO_SEG = float(os.environ.get("HISTORIAL_SONDEO", "5"))
LOTE = 100

_outbox = db.get_collection("outbox_eventos", write_concern=WriteConcern(w="majority"))
_despertar = threading.Event()
_hilo = None
_hilo_lock = threading.Lock()


def a_utc(valor):
    """datetime (naive de Mongo o con zona) -> ISO 8601 UTC, a milisegundos."""
    if valor is None:
        return None
    if not isinstance(valor, datetime):
        return str(valor)
    if valor.tzinfo is None:
        valor = valor.replace(tzinfo=timezone.utc)
    else:
        valor = valor.astimezone(timezone.utc)
    valor = valor.replace(microsecond=(valor.microsecond // 1000) * 1000)
    return valor.isoformat()


def _post(evento):
    """Devuelve 'ok' (202/200), 'invalido' (4xx) o 'reintentar' (red, 5xx, timeout)."""
    url = f"{HISTORIAL_URL.rstrip('/')}/eventos"
    req = request.Request(
        url, data=json.dumps(evento).encode(), method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with request.urlopen(req, timeout=TIMEOUT_SEG) as resp:
            return "ok" if 200 <= resp.status < 300 else "reintentar"
    except error.HTTPError as e:
        return "invalido" if 400 <= e.code < 500 else "reintentar"
    except Exception as e:  # noqa: BLE001  (conexion rechazada, timeout, DNS...)
        logger.warning("Historial no disponible (%s: %s)", type(e).__name__, e)
        return "reintentar"


def _bucle():
    espera = ESPERA_MIN
    while True:
        _despertar.wait(timeout=SONDEO_SEG)
        _despertar.clear()
        while True:
            try:
                docs = list(_outbox.find({"estado": "pendiente"})
                            .sort("creado_en", 1).limit(LOTE))
            except PyMongoError as e:
                logger.warning("Outbox: MongoDB no disponible (%s)", type(e).__name__)
                time.sleep(espera)
                espera = min(espera * 2, ESPERA_MAX)
                break
            if not docs:
                espera = ESPERA_MIN
                break
            fallo = False
            for d in docs:
                res = _post(d["evento"])
                try:
                    if res == "ok":
                        _outbox.delete_one({"_id": d["_id"]})
                    elif res == "invalido":
                        _outbox.update_one({"_id": d["_id"]},
                                           {"$set": {"estado": "invalido"}})
                        logger.error("Historial rechazo el evento %s como invalido", d["_id"])
                    else:
                        _outbox.update_one({"_id": d["_id"]}, {"$inc": {"intentos": 1}})
                        fallo = True
                        break          # conserva el orden: no se salta el evento pendiente
                except PyMongoError:
                    fallo = True
                    break
            if fallo:
                logger.warning("Historial: entrega pendiente, reintento en %.1f s", espera)
                time.sleep(espera)
                espera = min(espera * 2, ESPERA_MAX)
            else:
                espera = ESPERA_MIN


def _asegurar_hilo():
    global _hilo
    with _hilo_lock:
        if _hilo is None or not _hilo.is_alive():
            _hilo = threading.Thread(target=_bucle, name="outbox-historial", daemon=True)
            _hilo.start()


def emitir_evento(
    *, evento_id, timestamp, tarea, estado_anterior, estado_nuevo,
    anotador_id=None, revisor_id=None, codigo_motivo=None,
    toma_ts=None, duracion_seg=None, version=None,
):
    """Guarda el evento en el outbox y avisa al hilo de entrega. Nunca lanza excepciones.

    `tarea` es el documento de MongoDB (proyecto_id, batch_id, modalidad, tipo).
    """
    if not ACTIVO:
        return
    try:
        evento = {
            "evento_id": str(evento_id),
            "timestamp": a_utc(timestamp),
            "version": version if version is not None else tarea.get("version"),
            "tarea_id": tarea["_id"],
            "proyecto_id": tarea.get("proyecto_id"),
            "batch_id": tarea.get("batch_id"),
            "modalidad": tarea.get("modalidad"),
            "tipo_tarea": tarea.get("tipo"),
            "estado_anterior": estado_anterior,
            "estado_nuevo": estado_nuevo,
            "anotador_id": str(anotador_id) if anotador_id else None,
            "revisor_id": str(revisor_id) if revisor_id else None,
            "codigo_motivo": codigo_motivo,
            "toma_ts": a_utc(toma_ts),
            "duracion_seg": duracion_seg,
        }
        _asegurar_hilo()
        _outbox.update_one(
            {"_id": evento["evento_id"]},
            {"$setOnInsert": {"evento": evento, "estado": "pendiente", "intentos": 0,
                              "creado_en": datetime.now(timezone.utc)}},
            upsert=True,
        )
        _despertar.set()
    except Exception:  # noqa: BLE001
        logger.exception("Historial: no se pudo guardar el evento en el outbox")


if ACTIVO:
    _asegurar_hilo()       # al arrancar, drena lo que haya quedado pendiente