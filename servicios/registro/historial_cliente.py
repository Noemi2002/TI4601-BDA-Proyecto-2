"""Cliente para enviar eventos de transicion al servicio de Historial.

Reglas de diseno:
- Se llama DESPUES de que MongoDB confirma la transicion.
- Nunca rompe ni retrasa la respuesta al cliente: el envio corre en un hilo
  aparte y cualquier error solo se registra en logs.
- Reintenta con el MISMO evento_id; el Historial deduplica por evento_id.
"""
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from urllib import request

logger = logging.getLogger("registro.historial")

HISTORIAL_URL = os.environ.get("HISTORIAL_URL", "http://historial:8003")
TIMEOUT_SEG = float(os.environ.get("HISTORIAL_TIMEOUT", "2"))
REINTENTOS = int(os.environ.get("HISTORIAL_REINTENTOS", "3"))
ACTIVO = os.environ.get("HISTORIAL_ACTIVO", "true").lower() != "false"


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


def _enviar(evento):
    url = f"{HISTORIAL_URL.rstrip('/')}/eventos"
    cuerpo = json.dumps(evento).encode()
    for intento in range(REINTENTOS):
        try:
            req = request.Request(
                url, data=cuerpo, method="POST",
                headers={"Content-Type": "application/json"},
            )
            with request.urlopen(req, timeout=TIMEOUT_SEG) as resp:
                if 200 <= resp.status < 300:
                    return True
        except Exception as error:  # noqa: BLE001
            logger.warning(
                "Historial: intento %s/%s fallido evento_id=%s: %s",
                intento + 1, REINTENTOS, evento["evento_id"], error,
            )
        time.sleep(0.5 * (2 ** intento))
    logger.error(
        "Historial: evento PERDIDO evento_id=%s tarea_id=%s estado_nuevo=%s",
        evento["evento_id"], evento["tarea_id"], evento["estado_nuevo"],
    )
    return False


def emitir_evento(
    *, evento_id, timestamp, tarea, estado_anterior, estado_nuevo,
    anotador_id=None, revisor_id=None, codigo_motivo=None,
    toma_ts=None, duracion_seg=None, version=None,
):
    """Arma el evento y lo envia en segundo plano. Nunca lanza excepciones.

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
        threading.Thread(target=_enviar, args=(evento,), daemon=True).start()
    except Exception:  # noqa: BLE001
        logger.exception("Historial: no se pudo armar el evento")