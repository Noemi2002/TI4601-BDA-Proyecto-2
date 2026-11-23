"""Genera el historial sintetico de eventos (~800 000) en Parquet.

Cada "tarea sintetica" recorre el mismo ciclo que el Registro:
  disponible -> abierta -> anotada -> en_revision -> aprobada | rechazada
y tras un rechazo puede volver a tomarse (hasta MAX_INTENTOS veces).

Los clientes, proyectos, anotadores, revisores y equipos son los REALES de
datos/*.jsonl; los tarea_id son sinteticos (no chocan con las 60 000 de Mongo).

Uso (desde la raiz del repo, con el clúster Spark arriba):
  docker exec proyecto2-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/generar_eventos.py [--tareas 175000] [--hasta AAAA-MM-DD]

Salida: /datalake/eventos/mes=AAAA-MM/part-*.parquet
"""
import argparse
import json
import math
import random
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# ---- Parametros del modelo -------------------------------------------------
P_RECHAZO = 0.15        # de las tareas revisadas, fraccion rechazada
P_REINTENTO = 0.85      # tras un rechazo, probabilidad de volver a tomarse
MAX_INTENTOS = 4
DIAS_HISTORIAL = 240    # ventana hacia atras desde --hasta
# lognormal (mu, sigma) de la duracion de anotacion, en ln(segundos)
DURACION = {
    "texto": (4.6, 0.5),    # ~100 s
    "imagen": (5.4, 0.5),   # ~220 s
    "video": (6.2, 0.5),    # ~490 s
    "lidar": (6.9, 0.5),    # ~990 s
}
ESPERA_REVISION = (math.log(4 * 3600), 1.5)   # mediana 4 h, cola larga (SLA)
DURACION_REVISION = (math.log(180), 0.6)      # mediana 3 min
PAUSA_REINTENTO = (math.log(3600), 1.0)       # mediana 1 h
MOTIVOS = ["ETIQUETA_INCORRECTA", "GEOMETRIA_IMPRECISA",
           "OBJETO_OMITIDO", "ARCHIVO_ILEGIBLE", "INSTRUCCION_NO_SEGUIDA"]

CAMPOS = [
    "evento_id", "timestamp", "mes", "version", "tarea_id", "batch_id",
    "proyecto_id", "cliente_id", "modalidad", "tipo_tarea", "equipo_id",
    "anotador_id", "revisor_id", "estado_anterior", "estado_nuevo",
    "codigo_motivo", "duracion_seg", "toma_ts", "sla_seg",
]


# ---- Datos base ------------------------------------------------------------
def leer_jsonl(ruta):
    with open(ruta, encoding="utf-8") as f:
        for linea in f:
            if linea.strip():
                yield json.loads(linea)


def cargar_dimensiones(dir_datos):
    proyectos = [
        {"proyecto_id": p["_id"], "cliente_id": p["cliente_id"],
         "modalidad": p["modalidad"], "sla_seg": int(p["sla_horas"]) * 3600}
        for p in leer_jsonl(f"{dir_datos}/proyectos.jsonl")
        if p.get("estado", "activo") == "activo"
    ]
    batches = defaultdict(list)
    for b in leer_jsonl(f"{dir_datos}/batches.jsonl"):
        batches[b["proyecto_id"]].append(b["_id"])
    anotadores, revisores = [], []
    for a in leer_jsonl(f"{dir_datos}/anotadores.jsonl"):
        fila = {"id": a["_id"], "equipo_id": a.get("equipo_id")}
        (revisores if a.get("rol") == "revisor" else anotadores).append(fila)
    tipos = defaultdict(set)
    for t in leer_jsonl(f"{dir_datos}/tareas.jsonl"):
        tipos[t["modalidad"]].add(t["tipo"])
    if not anotadores or not revisores:
        raise SystemExit("anotadores.jsonl debe tener roles 'anotador' y 'revisor'")
    return {
        "proyectos": [p for p in proyectos if batches.get(p["proyecto_id"])],
        "batches": dict(batches),
        "anotadores": anotadores,
        "revisores": revisores,
        "tipos": {m: sorted(v) for m, v in tipos.items()},
    }


# ---- Generacion (funcion pura, probable sin Spark) -------------------------
def _uuid(rng):
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _seg(rng, par):
    return rng.lognormvariate(*par)


def generar_tarea(rng, dims, inicio, hasta):
    """Devuelve la lista de eventos (tuplas en el orden de CAMPOS) de una tarea."""
    proy = rng.choice(dims["proyectos"])
    modalidad = proy["modalidad"]
    tipos = dims["tipos"].get(modalidad) or ["generico"]
    tarea_id = _uuid(rng)
    base = {
        "tarea_id": tarea_id,
        "batch_id": rng.choice(dims["batches"][proy["proyecto_id"]]),
        "proyecto_id": proy["proyecto_id"],
        "cliente_id": proy["cliente_id"],
        "modalidad": modalidad,
        "tipo_tarea": rng.choice(tipos),
        "sla_seg": proy["sla_seg"],
    }
    margen = (hasta - inicio).total_seconds() - 3600
    t = inicio + timedelta(seconds=rng.random() * margen)
    eventos, version = [], 0

    def emitir(ts, anterior, nuevo, anot, **extra):
        nonlocal version
        version += 1
        fila = dict(base, evento_id=_uuid(rng), timestamp=ts,
                    mes=ts.strftime("%Y-%m"), version=version,
                    equipo_id=anot["equipo_id"], anotador_id=anot["id"],
                    revisor_id=None, estado_anterior=anterior,
                    estado_nuevo=nuevo, codigo_motivo=None,
                    duracion_seg=None, toma_ts=None)
        fila.update(extra)
        eventos.append(tuple(fila[c] for c in CAMPOS))

    for intento in range(MAX_INTENTOS):
        anot = rng.choice(dims["anotadores"])
        rev = rng.choice(dims["revisores"])
        toma = t
        emitir(toma, "disponible", "abierta", anot, toma_ts=toma)

        dur = _seg(rng, DURACION[modalidad])
        t = toma + timedelta(seconds=dur)
        if t > hasta:
            break
        emitir(t, "abierta", "anotada", anot, toma_ts=toma, duracion_seg=int(dur))

        t += timedelta(seconds=_seg(rng, ESPERA_REVISION))
        if t > hasta:
            break
        emitir(t, "anotada", "en_revision", anot, revisor_id=rev["id"])

        t += timedelta(seconds=_seg(rng, DURACION_REVISION))
        if t > hasta:
            break
        if rng.random() >= P_RECHAZO:
            emitir(t, "en_revision", "aprobada", anot,
                   revisor_id=rev["id"], toma_ts=toma)
            break
        emitir(t, "en_revision", "rechazada", anot, revisor_id=rev["id"],
               codigo_motivo=rng.choice(MOTIVOS))
        if rng.random() >= P_REINTENTO:
            break
        t += timedelta(seconds=_seg(rng, PAUSA_REINTENTO))
        if t > hasta:
            break
    return eventos


def particion(idx, it, dims, semilla, inicio, hasta):
    rng = random.Random(f"{semilla}-{idx}")
    for _ in it:
        yield from generar_tarea(rng, dims, inicio, hasta)


# ---- Spark -----------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tareas", type=int, default=175000)
    ap.add_argument("--hasta", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ap.add_argument("--semilla", type=int, default=4601)
    ap.add_argument("--datos", default="/datos")
    ap.add_argument("--salida", default="/datalake/eventos")
    ap.add_argument("--modo", choices=["overwrite", "append"], default="overwrite")
    ap.add_argument("--particiones", type=int, default=8)
    args = ap.parse_args()

    from pyspark.sql import SparkSession
    from pyspark.sql.types import (IntegerType, StringType, StructField,
                                   StructType, TimestampType)

    hasta = datetime.strptime(args.hasta, "%Y-%m-%d").replace(hour=23, minute=59)
    inicio = hasta - timedelta(days=DIAS_HISTORIAL)
    dims = cargar_dimensiones(args.datos)

    spark = (SparkSession.builder.appName("generar-eventos")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    sc = spark.sparkContext
    dims_b = sc.broadcast(dims)
    semilla, n = args.semilla, args.tareas

    schema = StructType([
        StructField(c, TimestampType() if c in ("timestamp", "toma_ts")
                    else IntegerType() if c in ("version", "duracion_seg", "sla_seg")
                    else StringType(), True)
        for c in CAMPOS
    ])
    rdd = (sc.parallelize(range(n), args.particiones)
           .mapPartitionsWithIndex(
               lambda i, it: particion(i, it, dims_b.value, semilla, inicio, hasta)))
    df = spark.createDataFrame(rdd, schema)
    (df.repartition("mes").write.mode(args.modo)
       .partitionBy("mes").parquet(args.salida))

    leido = spark.read.parquet(args.salida)
    print("TOTAL EVENTOS:", leido.count())
    leido.groupBy("estado_nuevo").count().orderBy("estado_nuevo").show()
    leido.groupBy("mes").count().orderBy("mes").show()
    spark.stop()


if __name__ == "__main__":
    main()