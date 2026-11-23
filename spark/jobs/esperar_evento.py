"""Mide cuantos segundos tarda un evento en ser VISIBLE DESDE SPARK.

Se deja corriendo ANTES de la prueba e2e. Arranca Spark, hace una consulta de
calentamiento (para no contar el costo de inicializacion) y espera a que
pruebas/e2e_rechazo.py escriba la senal {evento_id, t0} justo cuando recibe el
HTTP 200 del rechazo. Desde ese instante consulta el lago hasta ver el evento.

Solo LEE Parquet (no consulta MongoDB ni el Historial).

  docker exec proyecto2-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 /opt/spark/jobs/esperar_evento.py \
    | tee evidence/p4-visible-en-spark.txt

El resultado es una cota superior: incluye la duracion de la consulta que
encuentra el evento (se informa por separado) y el intervalo entre consultas.
"""
import argparse
import json
import os
import time

from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--lago", default="/datalake/eventos")
ap.add_argument("--senal", default="/datalake/_senal_e2e.json")
ap.add_argument("--timeout", type=int, default=300, help="segundos esperando la senal")
ap.add_argument("--max-visible", type=int, default=120, help="segundos esperando el evento")
args = ap.parse_args()

if os.path.exists(args.senal):
    os.remove(args.senal)

spark = (SparkSession.builder.appName("esperar-evento")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def consulta(eid):
    return (spark.read.parquet(args.lago)          # lectura nueva = lista los archivos otra vez
            .where(F.col("evento_id") == eid).count())


consulta("calentamiento")                           # paga el arranque de Spark antes de medir
print("LISTO: Spark calentado, esperando la senal de la prueba e2e...", flush=True)

t_inicio = time.time()
while not os.path.exists(args.senal):
    if time.time() - t_inicio > args.timeout:
        raise SystemExit("Se agoto el tiempo esperando la senal.")
    time.sleep(0.2)
time.sleep(0.2)
with open(args.senal) as f:
    senal = json.load(f)
eid, t0 = senal["evento_id"], senal["t0"]
print(f"Senal recibida: evento {eid}", flush=True)

consultas, dur_ultima = 0, None
while time.time() - t0 < args.max_visible:
    ini = time.time()
    n = consulta(eid)
    dur_ultima = time.time() - ini
    consultas += 1
    if n >= 1:
        visible = time.time() - t0
        print(f"VISIBLE EN SPARK: {visible:.1f} s despues del HTTP 200 "
              f"({consultas} consultas, la ultima duro {dur_ultima:.1f} s)")
        print("RESUMEN_SPARK_JSON", json.dumps({
            "evento_id": eid, "segundos_http200_a_spark": round(visible, 2),
            "consultas": consultas, "duracion_ultima_consulta_s": round(dur_ultima, 2),
            "executors": len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()) - 1}))
        break
else:
    print("NO VISIBLE: el evento no aparecio dentro del limite.")
os.remove(args.senal)
spark.stop()