
import json
import os
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

URI = os.environ["MONGO_URI"]
BASE = "http://registro:8001"


def post(ruta, datos):
    peticion = Request(
        BASE + ruta,
        data=json.dumps(datos).encode(),
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urlopen(peticion, timeout=30) as respuesta:
            return respuesta.status, json.loads(respuesta.read())

    except HTTPError as error:
        return error.code, json.loads(error.read())


def main():
    proyecto_id = str(uuid4())
    batch_id = str(uuid4())
    tarea_id = str(uuid4())

    with MongoClient(URI) as cliente:
        db = cliente.get_database(
            "plataforma",
            write_concern=WriteConcern(w="majority")
        )

        origen = db.clientes.find_one({
            "dataset_id": "principal-v1"
        })

        usuarios = list(db.anotadores.find({
            "dataset_id": "principal-v1",
            "rol": "anotador",
            "estado": "activo"
        }).limit(2))

        assert origen is not None
        assert len(usuarios) == 2

        comando_proyecto = {
            "accion": "crear_proyecto",
            "proyecto_id": proyecto_id,
            "cliente_id": origen["_id"],
            "nombre": "Prueba interna de comandos",
            "modalidad": "lidar",
            "geometria": "cuboide_3d",
            "taxonomia": [
                {
                    "etiqueta": "lidar#vehiculo",
                    "instruccion": "Marcar vehiculos"
                }
            ],
            "sla_horas": 48,
            "fecha_limite": (
                datetime.now(timezone.utc)
                + timedelta(days=30)
            ).isoformat()
        }

        comando_lote = {
            "accion": "crear_lote",
            "proyecto_id": proyecto_id,
            "batch_id": batch_id,
            "nombre": "Lote de prueba",
            "tareas": [
                {
                    "tarea_id": tarea_id,
                    "tipo": "deteccion_3d",
                    "prioridad": 3,
                    "archivo": {
                        "uri": "s3://datos-sinteticos/ensayo.bin",
                        "sha256": "a" * 64,
                        "bytes": 1024,
                        "mime": "application/octet-stream",
                        "meta": {"puntos": 100}
                    }
                }
            ]
        }

        try:
            # Crear proyecto y repetir solicitud
            codigo, r = post("/comandos", comando_proyecto)

            print("HTTP recibido:", codigo)
            print("Respuesta:", r)

            assert codigo == 200, f"HTTP inesperado: {codigo}, respuesta: {r}"
            assert r["creado"] is True

            codigo, r = post("/comandos", comando_proyecto)
            assert codigo == 200 and r["creado"] is False

            print("OK: proyecto creado sin duplicados")

            # Crear lote y repetir solicitud
            codigo, r = post("/comandos", comando_lote)
            assert codigo == 200 and r["creado"] is True

            codigo, r = post("/comandos", comando_lote)
            assert codigo == 200 and r["creado"] is False

            assert db.tareas.count_documents({
                "batch_id": batch_id
            }) == 1

            print("OK: lote y tarea creados sin duplicados")

            # Adquirir la tarea
            token = str(uuid4())

            codigo, r = post("/tareas/siguiente", {
                "proyecto_id": proyecto_id,
                "anotador_id": usuarios[0]["_id"],
                "token": token
            })

            assert codigo == 200
            assert r["tarea_id"] == tarea_id

            # Intento de liberacion con token incorrecto
            liberar = {
                "accion": "liberar_tarea",
                "tarea_id": tarea_id,
                "anotador_id": usuarios[0]["_id"],
                "token": str(uuid4())
            }

            codigo, _ = post("/comandos", liberar)
            assert codigo == 409

            # Liberacion correcta
            liberar["token"] = token

            codigo, r = post("/comandos", liberar)
            assert codigo == 200
            assert r["estado"] == "disponible"

            # Segunda liberacion
            codigo, _ = post("/comandos", liberar)
            assert codigo == 409

            print("OK: liberacion y validacion de token")

            # Otro anotador adquiere la tarea
            codigo, r = post("/tareas/siguiente", {
                "proyecto_id": proyecto_id,
                "anotador_id": usuarios[1]["_id"],
                "token": str(uuid4())
            })

            assert codigo == 200
            assert r["tarea_id"] == tarea_id

            tarea = db.tareas.find_one({"_id": tarea_id})
            assert tarea["version"] == 3

            print("OK: tarea liberada y reasignada")
            print("OK: prueba de comandos completada")

        finally:
            db.tareas.delete_many({"batch_id": batch_id})
            db.batches.delete_one({"_id": batch_id})
            db.proyectos.delete_one({"_id": proyecto_id})

            print("Datos temporales eliminados")


if __name__ == "__main__":
    main()
