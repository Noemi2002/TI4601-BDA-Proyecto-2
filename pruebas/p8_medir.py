import argparse
import copy
import csv
import hashlib
import json
import math
import os
import sys
import time

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.write_concern import WriteConcern


ROOT = Path("/app/evidence")
URI = os.environ["MONGO_URI"]


def conexion():
    return MongoClient(
        URI,
        retryWrites=False,
        serverSelectionTimeoutMS=2500,
        connectTimeoutMS=1500,
        socketTimeoutMS=6000,
    )


def coleccion(db, w="majority"):
    return db.get_collection(
        "tareas",
        write_concern=WriteConcern(
            w=w,
            wtimeout=3000
        )
    )


def plantilla(db):
    doc = db.tareas.find_one({
        "dataset_id": "principal-v1"
    })

    if doc is None:
        raise RuntimeError(
            "Falta el conjunto principal-v1"
        )

    return doc


def documento(base, identificador, numero):
    doc = copy.deepcopy(base)

    uid = str(uuid4())

    doc["_id"] = uid
    doc["dataset_id"] = f"p8-{identificador}"

    doc["estado"] = "disponible"
    doc["asignacion"] = None
    doc["anotacion_vigente"] = None
    doc["ultima_revision"] = None
    doc["ultimo_evento_id"] = None
    doc["version"] = 0

    doc["archivo"]["uri"] = (
        f"p8://{identificador}/{numero}/{uid}"
    )

    doc["archivo"]["sha256"] = hashlib.sha256(
        uid.encode()
    ).hexdigest()

    doc["creado_en"] = datetime.now(timezone.utc)
    doc["actualizado_en"] = datetime.now(timezone.utc)

    return doc


def escribir_jsonl(ruta, item):
    with ruta.open("a", encoding="utf-8") as archivo:
        archivo.write(
            json.dumps(item, ensure_ascii=False) + "\n"
        )


def leer_jsonl(ruta):
    if not ruta.exists():
        raise RuntimeError(
            f"No existe el archivo {ruta}"
        )

    return [
        json.loads(linea)
        for linea in ruta.read_text().splitlines()
        if linea.strip()
    ]


def sonda(args):
    ROOT.mkdir(
        exist_ok=True,
        parents=True
    )

    ruta = ROOT / f"p8_sonda_{args.id}.jsonl"
    detener = ROOT / f"p8_detener_{args.id}.flag"

    if ruta.exists() or detener.exists():
        raise RuntimeError(
            "Ya existe evidencia de esta ejecución. "
            "Utiliza otro identificador."
        )

    with conexion() as cliente:
        db = cliente["plataforma"]

        base = plantilla(db)
        tareas = coleccion(db)

        inicio = time.monotonic()
        numero = 0

        while (
            not detener.exists()
            and time.monotonic() - inicio < args.max_segundos
        ):
            numero += 1

            doc = documento(
                base,
                args.id,
                numero
            )

            ts_inicio = time.time()
            momento = time.perf_counter()
            error = ""

            try:
                tareas.insert_one(doc)
                estado = "ACK"

            except PyMongoError as exc:
                estado = "ERROR"
                error = type(exc).__name__

            ts_fin = time.time()

            escribir_jsonl(
                ruta,
                {
                    "numero": numero,
                    "id": doc["_id"],
                    "inicio_epoch": ts_inicio,
                    "fin_epoch": ts_fin,
                    "latencia_ms": round(
                        (
                            time.perf_counter()
                            - momento
                        ) * 1000,
                        3
                    ),
                    "estado": estado,
                    "error": error,
                }
            )

            time.sleep(args.intervalo)

    print(
        f"Sonda completada: {numero} intentos; "
        f"archivo {ruta}",
        flush=True
    )


def percentil(valores, porcentaje):
    orden = sorted(valores)

    indice = max(
        0,
        math.ceil(
            len(orden) * porcentaje / 100
        ) - 1
    )

    return orden[indice]


def verificar(args):
    ruta = ROOT / f"p8_sonda_{args.id}.jsonl"

    marcadores = (
        ROOT / f"p8_marcas_{args.id}.jsonl"
    )

    filas = leer_jsonl(ruta)
    eventos = leer_jsonl(marcadores)

    confirmadas = [
        f for f in filas
        if f["estado"] == "ACK"
    ]

    fallidas = [
        f for f in filas
        if f["estado"] == "ERROR"
    ]

    if not confirmadas:
        raise RuntimeError(
            "No hubo escrituras ACK; "
            "no puede medirse RPO"
        )

    with conexion() as cliente:
        db = cliente["plataforma"]

        ids = [
            f["id"]
            for f in confirmadas
        ]

        almacenadas = set()

        for i in range(0, len(ids), 500):
            cursor = db.tareas.find(
                {
                    "_id": {
                        "$in": ids[i:i + 500]
                    },
                    "dataset_id": f"p8-{args.id}"
                },
                {"_id": 1}
            )

            almacenadas.update(
                doc["_id"]
                for doc in cursor
            )

    faltantes = sorted(
        set(ids) - almacenadas
    )

    print(
        f"Intentos de la sonda: {len(filas)}"
    )

    print(
        "Escrituras ACK con w=majority: "
        f"{len(confirmadas)}"
    )

    print(
        "Intentos sin confirmación: "
        f"{len(fallidas)}"
    )

    print(
        "RPO observado "
        "(escrituras ACK no encontradas): "
        f"{len(faltantes)}"
    )

    print(
        "Nota: ERROR no significa necesariamente "
        "que la escritura no exista."
    )

    # Calcular interrupción observada
    tiempos = {
        e["evento"]: e["ts"]
        for e in eventos
    }

    if "primario_caido" in tiempos:
        t = tiempos["primario_caido"]

        previas = [
            f["fin_epoch"]
            for f in confirmadas
            if f["fin_epoch"] < t
        ]

        posteriores = [
            f["fin_epoch"]
            for f in confirmadas
            if f["fin_epoch"] >= t
        ]

        if previas and posteriores:
            interrupcion = max(
                0,
                min(posteriores) - max(previas)
            )

            recuperacion = max(
                0,
                min(posteriores) - t
            )

            print(
                "Interrupción observada entre ACK "
                "(RTO aproximado): "
                f"{interrupcion:.3f} s"
            )

            print(
                "Tiempo desde caída confirmada "
                "a primer ACK: "
                f"{recuperacion:.3f} s"
            )

        else:
            print(
                "RTO: no se pudo acotar "
                "(faltan ACK antes o después)"
            )

    # Comprobar las fases del experimento
    for prefijo in (
        "secundario",
        "sin_mayoria"
    ):
        comienzo = tiempos.get(
            f"{prefijo}_inicio"
        )

        fin = tiempos.get(
            f"{prefijo}_fin"
        )

        if comienzo is None or fin is None:
            raise RuntimeError(
                f"Faltan marcas de tiempo para {prefijo}"
            )

        dentro = [
            f for f in filas
            if comienzo <= f["fin_epoch"] < fin
        ]

        resumen = {
            k: sum(
                f["estado"] == k
                for f in dentro
            )
            for k in ("ACK", "ERROR")
        }

        print(
            f"Fase {prefijo}: {resumen} "
            f"(intervalo observado: "
            f"{fin - comienzo:.1f} s)"
        )

        if (
            prefijo == "secundario"
            and resumen["ACK"] == 0
        ):
            raise RuntimeError(
                "No se observaron escrituras "
                "confirmadas durante la caída "
                "del secundario"
            )

        if prefijo == "sin_mayoria":
            if (
                resumen["ACK"] != 0
                or resumen["ERROR"] == 0
            ):
                raise RuntimeError(
                    "La fase sin mayoría no produjo "
                    "el resultado esperado"
                )

    if faltantes:
        print(
            f"EJEMPLOS FALTANTES: {faltantes[:5]}"
        )
        sys.exit(2)

    print(
        "OK: ninguna escritura confirmada "
        "por mayoría se perdió"
    )


def benchmark(args):
    ROOT.mkdir(
        exist_ok=True,
        parents=True
    )

    ruta = (
        ROOT / f"p8_benchmark_{args.id}.csv"
    )

    with conexion() as cliente:
        db = cliente["plataforma"]

        base = plantilla(db)

        niveles = [
            ("w1", 1),
            ("majority", "majority")
        ]

        latencias = {
            "w1": [],
            "majority": []
        }

        with ruta.open(
            "w",
            newline="",
            encoding="utf-8"
        ) as archivo:
            writer = csv.writer(archivo)

            writer.writerow([
                "nivel",
                "muestra",
                "latencia_ms",
                "estado"
            ])

            for i in range(args.muestras):
                orden_niveles = (
                    niveles
                    if i % 2 == 0
                    else list(reversed(niveles))
                )

                for etiqueta, w in orden_niveles:
                    doc = documento(
                        base,
                        args.id,
                        f"bench-{etiqueta}-{i}"
                    )

                    tiempo = time.perf_counter()

                    try:
                        coleccion(
                            db,
                            w=w
                        ).insert_one(doc)

                        estado = "ACK"

                    except PyMongoError as exc:
                        estado = type(exc).__name__

                    duracion = (
                        time.perf_counter() - tiempo
                    ) * 1000

                    writer.writerow([
                        etiqueta,
                        i,
                        round(duracion, 3),
                        estado
                    ])

                    if estado == "ACK":
                        latencias[etiqueta].append(
                            duracion
                        )

    print(
        f"Benchmark guardado: {ruta}"
    )

    for etiqueta in (
        "w1",
        "majority"
    ):
        datos = latencias[etiqueta]

        print(
            f"{etiqueta}: "
            f"{len(datos)}/{args.muestras} "
            "confirmadas"
        )

        if datos:
            print(
                f"  p50={percentil(datos, 50):.2f} ms; "
                f"p95={percentil(datos, 95):.2f} ms; "
                f"p99={percentil(datos, 99):.2f} ms"
            )

        if len(datos) != args.muestras:
            raise RuntimeError(
                "Benchmark incompleto "
                f"para {etiqueta}"
            )


def limpiar(args):
    with conexion() as cliente:
        db = cliente["plataforma"]

        resultado = coleccion(
            db
        ).delete_many({
            "dataset_id": f"p8-{args.id}"
        })

    print(
        "Documentos P8 eliminados: "
        f"{resultado.deleted_count}"
    )


def main():
    parser = argparse.ArgumentParser()

    subcomandos = parser.add_subparsers(
        dest="comando",
        required=True
    )

    for nombre in (
        "sonda",
        "verificar",
        "benchmark",
        "limpiar"
    ):
        sub = subcomandos.add_parser(nombre)

        sub.add_argument(
            "--id",
            required=True
        )

        if nombre == "sonda":
            sub.add_argument(
                "--intervalo",
                type=float,
                default=0.4
            )

            sub.add_argument(
                "--max-segundos",
                type=int,
                default=600
            )

        if nombre == "benchmark":
            sub.add_argument(
                "--muestras",
                type=int,
                default=100
            )

    args = parser.parse_args()

    operaciones = {
        "sonda": sonda,
        "verificar": verificar,
        "benchmark": benchmark,
        "limpiar": limpiar
    }

    operaciones[args.comando](args)


if __name__ == "__main__":
    main()