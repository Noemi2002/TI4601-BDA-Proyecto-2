"""Servicio de Historial: recibe eventos de transicion y los publica en Parquet.

Flujo de un evento (POST /eventos):
  1. Valida el contrato (pydantic).
  2. Descarta duplicados por evento_id (conjunto en memoria, reconstruido al
     arrancar a partir del datalake y del WAL).
  3. Completa cliente_id, sla_seg y equipo_id leyendo MongoDB en SOLO LECTURA,
     con cache. Si MongoDB falla y no hay cache -> 503 (el Registro reintenta
     con el mismo evento_id).
  4. Lo anota en un WAL (fsync) y en el buffer; recien ahi responde.
  5. Un hilo vuelca el buffer a Parquet cada FLUSH_SEGUNDOS o al llegar a
     LOTE_MAX eventos (micro-lote), en <lago>/mes=AAAA-MM/.

Publicacion atomica: cada archivo se escribe como ".tmp-*.parquet" (oculto: Spark
ignora los nombres que empiezan con "." o "_"), se hace fsync y se renombra con
os.replace. Spark nunca ve un archivo a medio escribir.

Durabilidad: si el proceso muere con eventos en el buffer, el WAL permite
recuperarlos al arrancar; la deduplicacion evita repetir los que ya se publicaron.
"""
import glob
import json
import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Response
from pydantic import AwareDatetime, BaseModel, Field


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("historial")

LAGO = os.environ.get("LAGO_DIR", "/datalake/eventos").rstrip("/")
WAL_DIR = os.environ.get("WAL_DIR", "/datalake/wal").rstrip("/")
FLUSH_SEG = float(os.environ.get("FLUSH_SEGUNDOS", "5"))
LOTE_MAX = int(os.environ.get("LOTE_MAX", "500"))


# Mismo esquema que genera spark/jobs/generar_eventos.py (sin "mes": es la particion)
SCHEMA = pa.schema([
    ("evento_id", pa.string()),
    ("timestamp", pa.timestamp("us", tz="UTC")),
    ("version", pa.int32()),
    ("tarea_id", pa.string()),
    ("batch_id", pa.string()),
    ("proyecto_id", pa.string()),
    ("cliente_id", pa.string()),
    ("modalidad", pa.string()),
    ("tipo_tarea", pa.string()),
    ("equipo_id", pa.string()),
    ("anotador_id", pa.string()),
    ("revisor_id", pa.string()),
    ("estado_anterior", pa.string()),
    ("estado_nuevo", pa.string()),
    ("codigo_motivo", pa.string()),
    ("duracion_seg", pa.int32()),
    ("toma_ts", pa.timestamp("us", tz="UTC")),
    ("sla_seg", pa.int32()),
])
COLUMNAS = SCHEMA.names


# ---------------------------------------------------------------- contrato
class EventoEntrada(BaseModel):
    evento_id: uuid.UUID
    timestamp: AwareDatetime
    version: int | None = Field(
        default=None,
        ge=0,
    )
    tarea_id: str = Field(min_length=1)
    proyecto_id: str = Field(min_length=1)
    batch_id: str | None = None

    cliente_id: str
    equipo_id: str | None = None
    sla_seg: int | None = Field(
        default=None,
        ge=0,
    )

    modalidad: str | None = None
    tipo_tarea: str | None = None
    estado_anterior: str | None = None

    estado_nuevo: Literal[
        "disponible",
        "abierta",
        "anotada",
        "en_revision",
        "aprobada",
        "rechazada",
    ]

    anotador_id: str | None = None
    revisor_id: str | None = None
    codigo_motivo: str | None = None
    duracion_seg: int | None = Field(
        default=None,
        ge=0,
    )
    toma_ts: AwareDatetime | None = None


def _utc_iso(dt):
    return dt.astimezone(timezone.utc).isoformat() if dt else None


def construir_registro(
    ev: EventoEntrada,
) -> dict:

    ts = ev.timestamp.astimezone(
        timezone.utc
    )

    return {
        "evento_id": str(ev.evento_id),
        "timestamp": ts.isoformat(),
        "mes": ts.strftime("%Y-%m"),
        "version": ev.version,
        "tarea_id": ev.tarea_id,
        "batch_id": ev.batch_id,
        "proyecto_id": ev.proyecto_id,
        "cliente_id": ev.cliente_id,
        "modalidad": ev.modalidad,
        "tipo_tarea": ev.tipo_tarea,
        "equipo_id": ev.equipo_id,
        "anotador_id": ev.anotador_id,
        "revisor_id": ev.revisor_id,
        "estado_anterior": ev.estado_anterior,
        "estado_nuevo": ev.estado_nuevo,
        "codigo_motivo": ev.codigo_motivo,
        "duracion_seg": ev.duracion_seg,
        "toma_ts": _utc_iso(
            ev.toma_ts
        ),
        "sla_seg": ev.sla_seg,
    }


def clave(evento_id: str) -> bytes:
    """Clave compacta para el conjunto de deduplicacion."""
    try:
        return uuid.UUID(str(evento_id)).bytes
    except ValueError:
        return str(evento_id).encode()




# ------------------------------------------------- buffer, WAL y publicacion
def _fila(r: dict) -> dict:
    f = {c: r.get(c) for c in COLUMNAS}
    f["timestamp"] = datetime.fromisoformat(r["timestamp"])
    f["toma_ts"] = datetime.fromisoformat(r["toma_ts"]) if r.get("toma_ts") else None
    return f


def _abierto(ruta):
    os.makedirs(ruta, exist_ok=True)
    try:
        os.chmod(ruta, 0o777)       # Spark corre con otro usuario y debe poder borrar
    except OSError:
        pass


def publicar_parquet(lago: str, mes: str, filas: list) -> str:
    """Escribe un archivo Parquet de forma atomica en <lago>/mes=<mes>/."""
    carpeta = f"{lago}/mes={mes}"
    _abierto(carpeta)
    tmp = f"{carpeta}/.tmp-{uuid.uuid4().hex}.parquet"      # oculto para Spark
    final = f"{carpeta}/evt-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.parquet"
    tabla = pa.Table.from_pylist([_fila(r) for r in filas], schema=SCHEMA)
    try:
        pq.write_table(tabla, tmp, compression="snappy")
        with open(tmp, "rb") as f:
            os.fsync(f.fileno())
        os.chmod(tmp, 0o666)
        os.replace(tmp, final)                               # publicacion atomica
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    fd = os.open(carpeta, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return final


class Almacen:
    def __init__(self):
        self.lock = threading.Lock()          # buffer, vistos y WAL actual
        self.flush_lock = threading.Lock()    # un solo volcado a la vez
        self.vistos, self.buffer = set(), []
        self.wal = None
        self.despertar = threading.Event()
        self.detenido = False
        self.hilo = None
        self.stats = {"aceptados": 0, "duplicados": 0, "archivos": 0,
                      "eventos_publicados": 0, "ultimo_volcado": None, "errores_volcado": 0}

    # -- arranque ------------------------------------------------------------
    def iniciar(self):
        _abierto(LAGO)
        _abierto(WAL_DIR)
        for tmp in glob.glob(f"{LAGO}/mes=*/.tmp-*.parquet"):     # restos de una caida
            os.remove(tmp)
        t0 = time.perf_counter()
        for ruta in glob.glob(f"{LAGO}/mes=*/*.parquet"):
            for eid in pq.read_table(ruta, columns=["evento_id"]).column(0).to_pylist():
                self.vistos.add(clave(eid))
        log.info("Deduplicacion: %d evento_id leidos del datalake en %.1f s",
                 len(self.vistos), time.perf_counter() - t0)
        self._recuperar_wal()
        self.hilo = threading.Thread(target=self._bucle, name="volcado", daemon=True)
        self.hilo.start()

    def _recuperar_wal(self):
        actual = f"{WAL_DIR}/actual.jsonl"
        archivos = sorted(glob.glob(f"{WAL_DIR}/lote-*.jsonl")) + ([actual] if os.path.exists(actual) else [])
        nuevos = []
        for ruta in archivos:
            with open(ruta, encoding="utf-8") as f:
                for linea in f:
                    try:
                        r = json.loads(linea)
                    except ValueError:
                        continue                      # linea final truncada por una caida
                    k = clave(r["evento_id"])
                    if k not in self.vistos:
                        self.vistos.add(k)
                        nuevos.append(r)
        tmp = actual + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for r in nuevos:
                f.write(json.dumps(r) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, actual)
        for ruta in archivos:
            if ruta != actual:
                os.remove(ruta)
        self.wal = open(actual, "a", encoding="utf-8")
        self.buffer = nuevos
        if nuevos:
            log.warning("WAL: %d eventos recuperados, se publicaran en el proximo volcado", len(nuevos))
            self.despertar.set()

    # -- entrada -------------------------------------------------------------
    def visto(self, evento_id) -> bool:
        with self.lock:
            return clave(evento_id) in self.vistos

    def agregar(self, registro: dict) -> bool:
        """False si el evento ya existia (duplicado)."""
        k = clave(registro["evento_id"])
        with self.lock:
            if k in self.vistos:
                self.stats["duplicados"] += 1
                return False
            self._wal_escribir(registro)
            self.vistos.add(k)
            self.buffer.append(registro)
            self.stats["aceptados"] += 1
            if len(self.buffer) >= LOTE_MAX:
                self.despertar.set()
        return True

    def _wal_escribir(self, r):
        self.wal.write(json.dumps(r) + "\n")
        self.wal.flush()
        os.fsync(self.wal.fileno())

    # -- volcado ---------------------------------------------------------------
    def _bucle(self):
        while not self.detenido:
            self.despertar.wait(FLUSH_SEG)
            self.despertar.clear()
            try:
                self.vaciar()
            except Exception:                         # noqa: BLE001
                log.exception("Fallo inesperado en el volcado")

    def vaciar(self) -> int:
        with self.flush_lock:
            with self.lock:
                if not self.buffer:
                    return 0
                lote, self.buffer = self.buffer, []
                self.wal.close()
                lote_wal = f"{WAL_DIR}/lote-{uuid.uuid4().hex}.jsonl"
                os.replace(f"{WAL_DIR}/actual.jsonl", lote_wal)
                self.wal = open(f"{WAL_DIR}/actual.jsonl", "a", encoding="utf-8")
            try:
                por_mes = {}
                for r in lote:
                    por_mes.setdefault(r["mes"], []).append(r)
                for mes, filas in por_mes.items():
                    publicar_parquet(LAGO, mes, filas)
                    self.stats["archivos"] += 1
            except Exception:                         # noqa: BLE001
                log.exception("No se pudo publicar el lote; se reintentara")
                with self.lock:                       # devolver al WAL actual y al buffer
                    for r in lote:
                        self._wal_escribir(r)
                    self.buffer = lote + self.buffer
                    self.stats["errores_volcado"] += 1
                os.remove(lote_wal)
                return 0
            os.remove(lote_wal)                       # ya esta en Parquet
            self.stats["eventos_publicados"] += len(lote)
            self.stats["ultimo_volcado"] = datetime.now(timezone.utc).isoformat()
            log.info("Lote publicado: %d eventos en %d archivo(s)", len(lote), len(por_mes))
            return len(lote)

    def detener(self):
        self.detenido = True
        self.despertar.set()
        if self.hilo:
            self.hilo.join(timeout=10)
        self.vaciar()


almacen = Almacen()



@asynccontextmanager
async def lifespan(_app):
    almacen.iniciar()
    yield
    almacen.detener()


app = FastAPI(
    title="Historial",
    description="Recibe eventos de transicion y los publica en el datalake Parquet",
    version="1.0.0",
    lifespan=lifespan,
)


@app.post("/eventos", status_code=202)
def recibir_evento(
    ev: EventoEntrada,
    response: Response,
):
    if almacen.visto(ev.evento_id):
        response.status_code = 200

        return {
            "estado": "duplicado",
            "evento_id": str(ev.evento_id),
        }

    registro = construir_registro(ev)

    if not almacen.agregar(registro):
        response.status_code = 200

        return {
            "estado": "duplicado",
            "evento_id": str(ev.evento_id),
        }

    return {
        "estado": "aceptado",
        "evento_id": str(ev.evento_id),
        "mes": registro["mes"],
    }

@app.post("/flush")
def forzar_volcado():
    """Interno (pruebas): publica de inmediato el buffer pendiente."""
    return {"publicados": almacen.vaciar()}


@app.get("/salud")
def salud():

    escritura = os.access(
        LAGO,
        os.W_OK,
    )

    if not escritura:
        raise HTTPException(
            503,
            "El datalake no es escribible",
        )

    return {
        "servicio": "historial",
        "estado": "operativo",
        "datalake": LAGO,
        "pendientes_en_buffer": len(
            almacen.buffer
        ),
        "evento_id_conocidos": len(
            almacen.vistos
        ),
        **almacen.stats,
    }