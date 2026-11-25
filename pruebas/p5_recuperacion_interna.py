
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
        data=json.dumps(datos).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(peticion, timeout=30) as respuesta:
            contenido = respuesta.read()
            return (
                respuesta.status,
                json.loads(contenido) if contenido else {}
            )

    except HTTPError as error:
        contenido = error.read()
        return (
            error.code,
            json.loads(contenido) if contenido else {}
        )


def comprobar(etapa, codigo, esperado, respuesta):
    assert codigo == esperado, (
        f"{etapa}: HTTP {codigo}, "
        f"se esperaba {esperado}. "
        f"Respuesta: {respuesta}"
    )


def main():
    proyecto_id = str(uuid4())
    batch_id = str(uuid4())
    tarea_id = str(uuid4())

    with MongoClient(
        URI,
        serverSelectionTimeoutMS=5000
    ) as cliente:

        db = cliente.get_database(
            "plataforma",
            write_concern=WriteConcern(
                w="majority",
                wtimeout=15000
            )
        )

        cliente_base = db.clientes.find_one({
            "dataset_id": "principal-v1"
        })

        anotadores = list(
            db.anotadores.find({
                "dataset_id": "principal-v1",
                "rol": "anotador",
                "estado": "activo"
            }).limit(2)
        )

        assert cliente_base is not None
        assert len(anotadores) == 2

        proyecto = {
            "accion": "crear_proyecto",
            "proyecto_id": proyecto_id,
            "cliente_id": cliente_base["_id"],
            "nombre": "Prueba de recuperacion",
            "modalidad": "lidar",
            "geometria": "cuboide_3d",
            "taxonomia": [{
                "etiqueta": "vehiculo",
                "instruccion": "Delimitar vehiculos 3D"
            }],
            "sla_horas": 48,
            "fecha_limite": (
                datetime.now(timezone.utc)
                + timedelta(days=7)
            ).isoformat()
        }

        lote = {
            "accion": "crear_lote",
            "batch_id": batch_id,
            "proyecto_id": proyecto_id,
            "nombre": "Lote temporal",
            "tareas": [{
                "tarea_id": tarea_id,
                "tipo": "deteccion_3d",
                "prioridad": 3,
                "archivo": {
                    "uri": f"s3://pruebas/{tarea_id}.bin",
                    "sha256": "b" * 64,
                    "bytes": 1024,
                    "mime": "application/octet-stream",
                    "meta": {"puntos": 100}
                }
            }]
        }

        try:
            # 1. Crear proyecto y lote
            codigo, r = post("/comandos", proyecto)
            comprobar("crear proyecto", codigo, 200, r)
            assert r["creado"] is True

            codigo, r = post("/comandos", lote)
            comprobar("crear lote", codigo, 200, r)
            assert r["creado"] is True

            print("OK: proyecto y lote temporales creados")

            # 2. Asignar tarea al primer anotador
            token_original = str(uuid4())

            codigo, r = post("/tareas/siguiente", {
                "proyecto_id": proyecto_id,
                "anotador_id": anotadores[0]["_id"],
                "token": token_original
            })

            comprobar("asignar tarea", codigo, 200, r)

            assert r["tarea_id"] == tarea_id
            assert r["version"] == 1

            vencimiento_anterior = db.tareas.find_one({
                "_id": tarea_id
            })["asignacion"]["vence_en"]

            print("OK: tarea asignada con version 1")

            # 3. Renovacion con token incorrecto
            renovar = {
                "accion": "renovar_asignacion",
                "tarea_id": tarea_id,
                "anotador_id": anotadores[0]["_id"],
                "token": str(uuid4())
            }

            codigo, r = post("/comandos", renovar)
            comprobar(
                "renovacion con token incorrecto",
                codigo, 409, r
            )

            # 4. Renovacion correcta
            renovar["token"] = token_original

            codigo, r = post("/comandos", renovar)
            comprobar("renovacion correcta", codigo, 200, r)

            assert r["version"] == 1

            vencimiento_nuevo = db.tareas.find_one({
                "_id": tarea_id
            })["asignacion"]["vence_en"]

            assert vencimiento_nuevo > vencimiento_anterior

            print(
                "OK: renovacion amplia el plazo "
                "y conserva version 1"
            )

            # 5. No debe recuperarse una tarea vigente
            recuperar = {
                "accion": "recuperar_vencida",
                "tarea_id": tarea_id,
                "token": token_original,
                "version": 1
            }

            codigo, r = post("/comandos", recuperar)
            comprobar(
                "recuperacion antes del vencimiento",
                codigo, 409, r
            )

            print("OK: tarea vigente no puede recuperarse")

            # 6. Simular vencimiento
            pasado = (
                datetime.now(timezone.utc)
                - timedelta(seconds=10)
            )

            resultado = db.tareas.update_one(
                {
                    "_id": tarea_id,
                    "asignacion.token": token_original
                },
                {
                    "$set": {
                        "asignacion.vence_en": pasado
                    }
                }
            )

            assert resultado.matched_count == 1

            print("OK: vencimiento temporal simulado")

            # 7. No se permite renovar una tarea vencida
            codigo, r = post("/comandos", renovar)

            comprobar(
                "renovar tarea vencida",
                codigo, 409, r
            )

            # 8. No se permite enviar la anotacion vencida
            codigo, r = post(
                f"/tareas/{tarea_id}/enviar",
                {
                    "anotador_id": anotadores[0]["_id"],
                    "token": token_original,
                    "anotacion": {
                        "geometria": "cuboide_3d",
                        "etiquetas": ["vehiculo"],
                        "elementos": [{
                            "etiqueta": "vehiculo",
                            "id": "obj-1"
                        }]
                    }
                }
            )

            comprobar(
                "enviar tarea vencida",
                codigo, 409, r
            )

            print(
                "OK: renovacion y envio vencidos "
                "devuelven HTTP 409"
            )

            # 9. Detectar tarea vencida
            codigo, r = post("/comandos", {
                "accion": "consultar_vencidas",
                "proyecto_id": proyecto_id,
                "limite": 10
            })

            comprobar(
                "consultar vencidas",
                codigo, 200, r
            )

            candidatas = [
                t for t in r["candidatas"]
                if t["tarea_id"] == tarea_id
            ]

            assert len(candidatas) == 1
            assert candidatas[0]["token"] == token_original
            assert candidatas[0]["version"] == 1

            print("OK: consulta detecta tarea vencida")

            # 10. Token incorrecto
            recuperar["token"] = str(uuid4())

            codigo, r = post("/comandos", recuperar)

            comprobar(
                "recuperacion con token incorrecto",
                codigo, 409, r
            )

            # 11. Version incorrecta
            recuperar["token"] = token_original
            recuperar["version"] = 2

            codigo, r = post("/comandos", recuperar)

            comprobar(
                "recuperacion con version incorrecta",
                codigo, 409, r
            )

            # 12. Recuperacion correcta
            recuperar["version"] = 1

            codigo, r = post("/comandos", recuperar)

            comprobar(
                "recuperacion correcta",
                codigo, 200, r
            )

            assert r["estado"] == "disponible"
            assert r["version"] == 2

            tarea = db.tareas.find_one({
                "_id": tarea_id
            })

            assert tarea["asignacion"] is None
            assert tarea["ultimo_evento_id"] == r["evento_id"]

            print(
                "OK: recuperacion devuelve a disponible "
                "con version 2"
            )

            # 13. No recuperar dos veces
            codigo, r = post("/comandos", recuperar)

            comprobar(
                "recuperacion duplicada",
                codigo, 409, r
            )

            print("OK: recuperacion duplicada impedida")

            # 14. Otro anotador adquiere la tarea
            token_nuevo = str(uuid4())

            codigo, r = post("/tareas/siguiente", {
                "proyecto_id": proyecto_id,
                "anotador_id": anotadores[1]["_id"],
                "token": token_nuevo
            })

            comprobar(
                "reasignacion",
                codigo, 200, r
            )

            assert r["tarea_id"] == tarea_id
            assert r["version"] == 3

            tarea = db.tareas.find_one({
                "_id": tarea_id
            })

            assert tarea["estado"] == "abierta"
            assert (
                tarea["asignacion"]["anotador_id"]
                == anotadores[1]["_id"]
            )
            assert (
                tarea["asignacion"]["token"]
                == token_nuevo
            )

            print(
                "OK: tarea recuperada asignada "
                "a otro anotador, version 3"
            )

            print(
                "OK: recuperacion por vencimiento "
                "probada internamente"
            )

        finally:
            # Limpiar solamente este conjunto temporal
            db.tareas.delete_many({
                "batch_id": batch_id
            })

            db.batches.delete_one({
                "_id": batch_id
            })

            db.proyectos.delete_one({
                "_id": proyecto_id
            })

            print("Datos temporales eliminados")


if __name__ == "__main__":
    main()
