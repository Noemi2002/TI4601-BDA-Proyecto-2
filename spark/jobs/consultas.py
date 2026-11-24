"""Consultas analiticas Q1-Q5 con Spark. Lee SOLO el datalake Parquet.

Uso (desde la raiz del repo):
  docker exec proyecto2-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 /opt/spark/jobs/consultas.py \
    [--q q1 q3 ...] [--repeticiones 3] [--explain] [--ref "AAAA-MM-DD HH:MM:SS"]

- Q1 usa como "hoy" el ultimo evento del datalake (o --ref), no la fecha del
  sistema, para que los resultados sean reproducibles.
- "Revisadas" = eventos con estado_nuevo aprobada o rechazada.
- Q5 mide desde la ULTIMA toma (toma_ts del evento aprobada) hasta la aprobacion.
- Cada resultado se guarda como CSV en <salida>/qN para validarlo con pandas.
"""
import argparse
import json
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F

FMT = "%Y-%m-%d %H:%M:%S"


def revisadas(ev):
    return ev.filter(F.col("estado_nuevo").isin("aprobada", "rechazada"))


def q1(ev, ref):
    """Aprobadas por dia y modalidad, ultimos 90 dias."""
    desde = ref - timedelta(days=90)
    return (ev.filter(F.col("mes") >= desde.strftime("%Y-%m"))      # poda de particiones
              .filter(F.col("estado_nuevo") == "aprobada")
              .filter(F.col("timestamp") > F.lit(desde.strftime(FMT)).cast("timestamp"))
              .groupBy(F.to_date("timestamp").cast("string").alias("dia"), "modalidad")
              .agg(F.count("*").alias("aprobadas"))
              .orderBy("dia", "modalidad"))


def q2(ev, ref):
    """Proporcion aprobadas / revisadas por equipo y mes."""
    return (revisadas(ev).groupBy("equipo_id", "mes")
            .agg(F.sum(F.when(F.col("estado_nuevo") == "aprobada", 1).otherwise(0)).alias("aprobadas"),
                 F.count("*").alias("revisadas"))
            .withColumn("proporcion", F.round(F.col("aprobadas") / F.col("revisadas"), 4))
            .orderBy("equipo_id", "mes"))


def q3(ev, ref):
    """Duracion media y percentil 95 por modalidad y tipo de tarea."""
    return (ev.filter((F.col("estado_nuevo") == "anotada") & F.col("duracion_seg").isNotNull())
            .groupBy("modalidad", "tipo_tarea")
            .agg(F.count("*").alias("n"),
                 F.round(F.avg("duracion_seg"), 2).alias("media_seg"),
                 F.round(F.expr("percentile(duracion_seg, 0.95)"), 2).alias("p95_seg"))
            .orderBy("modalidad", "tipo_tarea"))


def q4(ev, ref):
    """Proporcion rechazadas / revisadas por cliente y proyecto."""
    return (revisadas(ev).groupBy("cliente_id", "proyecto_id")
            .agg(F.sum(F.when(F.col("estado_nuevo") == "rechazada", 1).otherwise(0)).alias("rechazadas"),
                 F.count("*").alias("revisadas"))
            .withColumn("proporcion", F.round(F.col("rechazadas") / F.col("revisadas"), 4))
            .orderBy("cliente_id", "proyecto_id"))


def q5(ev, ref):
    """Toma -> aprobacion: p50 y p95 por cliente y % que incumplio el SLA."""
    ap = (ev.filter((F.col("estado_nuevo") == "aprobada") & F.col("toma_ts").isNotNull())
            .withColumn("espera_seg", F.col("timestamp").cast("double") - F.col("toma_ts").cast("double"))
            .withColumn("incumple", (F.col("espera_seg") > F.col("sla_seg")).cast("int")))
    return (ap.groupBy("cliente_id")
              .agg(F.count("*").alias("n"),
                   F.round(F.expr("percentile(espera_seg, 0.5)"), 2).alias("p50_seg"),
                   F.round(F.expr("percentile(espera_seg, 0.95)"), 2).alias("p95_seg"),
                   F.round(F.avg("incumple") * 100, 2).alias("pct_incumple_sla"))
              .orderBy("cliente_id"))


CONSULTAS = {"q1": q1, "q2": q2, "q3": q3, "q4": q4, "q5": q5}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--q", nargs="+", default=list(CONSULTAS), choices=list(CONSULTAS))
    ap.add_argument("--repeticiones", type=int, default=3)
    ap.add_argument("--explain", action="store_true", help="plan de Q1 y Q3")
    ap.add_argument("--ref", help='fecha de referencia "AAAA-MM-DD HH:MM:SS"')
    ap.add_argument("--lago", default="/datalake/eventos")
    ap.add_argument("--salida", default="/datalake/resultados")
    a = ap.parse_args()

    spark = (SparkSession.builder.appName("consultas-q1-q5")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    leer = lambda: spark.read.parquet(a.lago)          # unica fuente: Parquet

    if a.ref:
        ref_txt = a.ref
    else:
        ref_txt = leer().agg(F.date_format(F.max("timestamp"), "yyyy-MM-dd HH:mm:ss")).first()[0]
    ref = datetime.strptime(ref_txt, FMT)
    n_exec = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
    print(f"\nREF={ref_txt}  fuente={a.lago}  executors={n_exec}  "
          f"paralelismo={spark.sparkContext.defaultParallelism}\n")

    resumen = {}
    for nombre in a.q:
        fn = CONSULTAS[nombre]
        tiempos = []
        for _ in range(a.repeticiones):
            t0 = time.perf_counter()
            fn(leer(), ref).collect()
            tiempos.append(round(time.perf_counter() - t0, 3))
        resumen[nombre] = tiempos
        print(f"=== {nombre.upper()}: {fn.__doc__}")
        print(f"    tiempos (s): {tiempos}")
        df = fn(leer(), ref)
        if a.explain and nombre in ("q1", "q3"):
            df.explain("formatted")
        df.show(10, truncate=False)
        (df.coalesce(1).write.mode("overwrite").option("header", True)
           .csv(f"{a.salida}/{nombre}"))

    print("RESUMEN_TIEMPOS_JSON", json.dumps({"executors": n_exec, "ref": ref_txt, "tiempos_s": resumen}))
    spark.stop()


if __name__ == "__main__":
    main()