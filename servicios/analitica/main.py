"""Servicio Analitica: expone Q1-Q5 por REST ejecutandolas con Apache Spark.

- Lee UNICAMENTE el datalake Parquet (volumen montado en solo lectura).
  No usa MongoDB ni el Historial y no escribe resultados en el lago.
- Reutiliza las mismas funciones q1..q5 de spark/jobs/consultas.py (montado en
  /opt/spark/jobs), por lo que el resultado es identico al de spark-submit.
- La sesion de Spark se crea al primer pedido (el arranque se informa aparte en
  `arranque_spark_s`) y se libera tras IDLE_SEG sin uso para no ocupar los cores
  del cluster; los pedidos siguientes dentro de ese plazo no pagan el arranque.

Rutas internas (2):
  GET /salud
  GET /consultas/{q1..q5}?conjunto=principal|grande&ref=AAAA-MM-DD HH:MM:SS&limite=1000
"""
import os
import socket
import sys
import threading
import time
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query

try:
    import pyspark  # noqa: F401
except ImportError:      # imagen apache/spark: pyspark viene en $SPARK_HOME/python
    import glob
    _home = os.environ.get("SPARK_HOME", "/opt/spark")
    sys.path[:0] = [f"{_home}/python", *glob.glob(f"{_home}/python/lib/py4j-*-src.zip")]

sys.path.insert(0, os.environ.get("JOBS_DIR", "/opt/spark/jobs"))
import consultas as C  # noqa: E402  (q1..q5 compartidas con spark-submit)

SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
DRIVER_HOST = os.environ.get("SPARK_DRIVER_HOST", "analitica")
CORES_MAX = os.environ.get("SPARK_CORES_MAX", "2")
IDLE_SEG = float(os.environ.get("IDLE_SEG", "120"))
CONJUNTOS = {
    "principal": os.environ.get("LAGO_PRINCIPAL", "/datalake/eventos"),
    "grande": os.environ.get("LAGO_GRANDE", "/datalake/eventos_grande"),
}

app = FastAPI(title="Analitica (Spark, solo lectura de Parquet)", version="1.0.0")

_candado = threading.Lock()      # una consulta Spark a la vez
_spark = None
_ultimo_uso = 0.0


def _sesion():
    """Devuelve (sesion, segundos_de_arranque). Segundos = 0 si ya estaba activa."""
    global _spark, _ultimo_uso
    if _spark is not None:
        _ultimo_uso = time.time()
        return _spark, 0.0
    t0 = time.perf_counter()
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName("analitica-api").master(SPARK_MASTER)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.cores.max", CORES_MAX)
         .config("spark.driver.host", DRIVER_HOST)
         .config("spark.driver.bindAddress", "0.0.0.0")
         .config("spark.driver.port", os.environ.get("SPARK_DRIVER_PORT", "7078"))
         .config("spark.blockManager.port", os.environ.get("SPARK_BLOCK_PORT", "7079"))
         .config("spark.ui.enabled", "false"))
    _spark = b.getOrCreate()
    _spark.sparkContext.setLogLevel("ERROR")
    _ultimo_uso = time.time()
    return _spark, round(time.perf_counter() - t0, 3)


def _liberar_si_ocioso():
    global _spark
    while True:
        time.sleep(15)
        with _candado:
            if _spark is not None and time.time() - _ultimo_uso > IDLE_SEG:
                try:
                    _spark.stop()
                finally:
                    _spark = None


threading.Thread(target=_liberar_si_ocioso, daemon=True).start()


def _master_alcanzable():
    try:
        host, puerto = SPARK_MASTER.replace("spark://", "").split(":")
        with socket.create_connection((host, int(puerto)), timeout=2):
            return True
    except (OSError, ValueError):
        return False


@app.get("/salud")
def salud():
    lago = CONJUNTOS["principal"]
    legible = os.path.isdir(lago) and os.access(lago, os.R_OK)
    master = _master_alcanzable()
    return {
        "servicio": "analitica",
        "estado": "operativo" if (legible and master) else "degradado",
        "spark_master": SPARK_MASTER,
        "spark_master_alcanzable": master,
        "datalake": lago,
        "datalake_legible": legible,
        "sesion_spark_activa": _spark is not None,
    }


@app.get("/consultas/{consulta}")
def ejecutar(
    consulta: str,
    conjunto: str = Query("principal"),
    ref: Optional[str] = Query(None, description="AAAA-MM-DD HH:MM:SS"),
    limite: int = Query(1000, ge=1, le=10000),
):
    consulta = consulta.lower()
    if consulta not in C.CONSULTAS:
        raise HTTPException(404, f"Consulta desconocida: {consulta} (q1..q5)")
    if conjunto not in CONJUNTOS:
        raise HTTPException(422, f"conjunto debe ser uno de {sorted(CONJUNTOS)}")
    lago = CONJUNTOS[conjunto]
    if not os.path.isdir(lago):
        raise HTTPException(404, f"El conjunto '{conjunto}' no esta disponible en este entorno")
    if ref:
        try:
            ref_dt = datetime.strptime(ref, C.FMT)
        except ValueError:
            raise HTTPException(422, "ref debe tener el formato AAAA-MM-DD HH:MM:SS")

    with _candado:
        try:
            spark, arranque = _sesion()
            from pyspark.sql import functions as F
            leer = lambda: spark.read.parquet(lago)           # unica fuente: Parquet
            t0 = time.perf_counter()
            if not ref:
                ref_txt = leer().agg(
                    F.date_format(F.max("timestamp"), "yyyy-MM-dd HH:mm:ss")).first()[0]
                ref_dt = datetime.strptime(ref_txt, C.FMT)
            filas = C.CONSULTAS[consulta](leer(), ref_dt).limit(limite + 1).collect()
            tiempo = round(time.perf_counter() - t0, 3)
        except HTTPException:
            raise
        except Exception as error:  # noqa: BLE001
            global _spark
            try:
                if _spark is not None:
                    _spark.stop()
            finally:
                _spark = None
            raise HTTPException(503, f"Spark no pudo ejecutar la consulta: {type(error).__name__}") from error

    truncado = len(filas) > limite
    filas = filas[:limite]
    return {
        "consulta": consulta,
        "descripcion": C.CONSULTAS[consulta].__doc__,
        "conjunto": conjunto,
        "fuente": lago,
        "ref": ref_dt.strftime(C.FMT),
        "filas": len(filas),
        "truncado": truncado,
        "tiempo_s": tiempo,
        "arranque_spark_s": arranque,
        "resultado": [r.asDict() for r in filas],
    }