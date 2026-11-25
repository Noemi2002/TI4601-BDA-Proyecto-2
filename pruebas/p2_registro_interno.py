
import json
import os
from copy import deepcopy
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from time import perf_counter
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from uuid import uuid4

from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

URI = os.environ["MONGO_URI"]
URL = "http://registro:8001/tareas/siguiente"


def solicitar(proyecto_id, anotador_id):
    datos = {
        "proyecto_id": proyecto_id,
        "anotador_id": anotador_id,
        "token": str(uuid4())
    }

    peticion = Request(
        URL,
        data=json.dumps(datos).encode(),
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    inicio = perf_counter()

    try:
        with urlopen(peticion, timeout=30) as respuesta:
            cuerpo = respuesta.read()
            codigo = respuesta.status
            resultado = json.loads(cuerpo) if cuerpo else {}

    except HTTPError as error:
        codigo = error.code
        resultado = {"error": error.read().decode()}

    duracion = (perf_counter() - inicio) * 1000

    return codigo, resultado, anotador_id, duracion


def percentil(valores, p):
    ordenados = sorted(valores)
    indice = max(
        0,
        (len(ordenados) * p + 99) // 100 - 1
    )
    return ordenados[int(indice)]


def probar_envio(
    tarea_id,
    anotador_id,
    token,
    geometria,
    etiqueta
):
    datos = {
        "anotador_id": anotador_id,
        "token": token,
        "anotacion": {
            "geometria": geometria,
            "etiquetas": [etiqueta],
            "elementos": [
                {
                    "etiqueta": etiqueta,
                    "referencia": "objeto-1"
                }
            ]
        }
    }

    peticion = Request(
        f"http://registro:8001/tareas/{tarea_id}/enviar",
        data=json.dumps(datos).encode(),
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urlopen(
            peticion, timeout=30
        ) as respuesta:
            return respuesta.status

    except HTTPError as error:
        return error.code



def llamar_api(ruta, datos=None, metodo="POST"):
    cuerpo = (
        json.dumps(datos).encode()
        if datos is not None
        else None
    )

    peticion = Request(
        f"http://registro:8001{ruta}",
        data=cuerpo,
        headers={"Content-Type": "application/json"},
        method=metodo
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


def main():
    identificador = str(uuid4())
    conjunto = f"p2-interno-{identificador}"

    with MongoClient(URI) as cliente:
        db = cliente.get_database(
            "plataforma",
            write_concern=WriteConcern(
                w="majority",
                wtimeout=15000
            )
        )

        # Datos existentes que sirven como plantilla
        proyecto_base = db.proyectos.find_one({
            "dataset_id": "principal-v1"
        })

        batch_base = db.batches.find_one({
            "proyecto_id": proyecto_base["_id"]
        })

        tarea_base = db.tareas.find_one({
            "batch_id": batch_base["_id"]
        })

        usuarios = list(
            db.anotadores.find({
                "dataset_id": "principal-v1",
                "rol": "anotador",
                "estado": "activo"
            }).limit(50)
        )

        assert len(usuarios) == 50

        proyecto_id = str(uuid4())
        batch_id = str(uuid4())

        proyecto = deepcopy(proyecto_base)
        proyecto["_id"] = proyecto_id
        proyecto["dataset_id"] = conjunto
        proyecto["nombre"] = "Prueba de concurrencia"
        proyecto["estado"] = "activo"

        batch = deepcopy(batch_base)
        batch["_id"] = batch_id
        batch["proyecto_id"] = proyecto_id
        batch["dataset_id"] = conjunto

        tareas = []

        for _ in range(10):
            tarea = deepcopy(tarea_base)
            tarea["_id"] = str(uuid4())
            tarea["dataset_id"] = conjunto
            tarea["proyecto_id"] = proyecto_id
            tarea["batch_id"] = batch_id
            tarea["estado"] = "disponible"
            tarea["asignacion"] = None
            tarea["ultimo_evento_id"] = None
            tarea["version"] = 0
            tareas.append(tarea)

        try:
            # Crear conjunto temporal
            db.proyectos.insert_one(proyecto)
            db.batches.insert_one(batch)
            db.tareas.insert_many(tareas)

            print("Fecha:", datetime.now(
                timezone.utc
            ).isoformat())
            print("Solicitudes: 50")
            print("Tareas disponibles: 10")

            # Ejecutar solicitudes concurrentemente
            with ThreadPoolExecutor(max_workers=50) as executor:
                futuros = [
                    executor.submit(
                        solicitar,
                        proyecto_id,
                        usuario["_id"]
                    )
                    for usuario in usuarios
                ]

                resultados = [
                    futuro.result()
                    for futuro in futuros
                ]

            conteos = Counter(
                codigo for codigo, _, _, _ in resultados
            )

            asignadas = [
                (respuesta, anotador_id)
                for codigo, respuesta, anotador_id, _
                in resultados
                if codigo == 200
            ]

            ids = [
                respuesta["tarea_id"]
                for respuesta, _ in asignadas
            ]

            # Verificar que cada propietario sea correcto
            for respuesta, anotador_id in asignadas:
                tarea = db.tareas.find_one({
                    "_id": respuesta["tarea_id"]
                })

                assert tarea["estado"] == "abierta"
                assert tarea["asignacion"][
                    "anotador_id"
                ] == anotador_id

            print("Respuestas HTTP:", dict(conteos))
            print("IDs asignados:", len(set(ids)))

            duraciones = [
                duracion
                for _, _, _, duracion in resultados
            ]

            for p in (50, 95, 99):
                print(
                    f"p{p}: "
                    f"{percentil(duraciones, p):.2f} ms"
                )

            assert conteos[200] == 10
            assert conteos[204] == 40
            assert len(set(ids)) == 10

            print("OK: 10 asignaciones sin duplicados")

            
            # Seleccionar una tarea adquirida correctamente
            respuesta, anotador_id = asignadas[0]

            tarea_id = respuesta["tarea_id"]
            token = respuesta["asignacion"]["token"]

            geometria = tarea_base["geometria"]
            etiqueta = proyecto_base["taxonomia"][0]["etiqueta"]

            # 1. Envio con token incorrecto
            codigo_incorrecto = probar_envio(
                tarea_id,
                anotador_id,
                str(uuid4()),
                geometria,
                etiqueta
            )

            assert codigo_incorrecto == 409
            print("OK: token incorrecto devuelve HTTP 409")

            # 2. Primer envio correcto
            codigo_primero = probar_envio(
                tarea_id,
                anotador_id,
                token,
                geometria,
                etiqueta
            )

            assert codigo_primero == 200
            print("OK: primer envio devuelve HTTP 200")

            # 3. Segundo envio de la misma tarea
            codigo_segundo = probar_envio(
                tarea_id,
                anotador_id,
                token,
                geometria,
                etiqueta
            )

            assert codigo_segundo == 409
            print("OK: segundo envio devuelve HTTP 409")

            # Verificar el estado definitivo
            tarea_final = db.tareas.find_one({
                "_id": tarea_id
            })

            assert tarea_final["estado"] == "anotada"
            assert tarea_final["version"] == 2

            print("OK: tarea anotada y version incrementada")


            # Obtener un revisor activo
            revisor = db.anotadores.find_one({
                "dataset_id": "principal-v1",
                "rol": "revisor",
                "estado": "activo"
            })

            assert revisor is not None
            revisor_id = revisor["_id"]


            def revisar(id_tarea, accion, motivo=None):
                datos = {
                    "revisor_id": revisor_id,
                    "accion": accion
                }

                if motivo is not None:
                    datos["codigo_motivo"] = motivo

                return llamar_api(
                    f"/tareas/{id_tarea}/revision",
                    datos
                )


            # -----------------------------------------
            # Caso 1: aprobar la primera tarea enviada
            # -----------------------------------------

            codigo, _ = revisar(tarea_id, "iniciar")
            assert codigo == 200

            estado = db.tareas.find_one({"_id": tarea_id})
            assert estado["estado"] == "en_revision"
            assert estado["version"] == 3

            print("OK: inicio de revision")

            codigo, _ = revisar(tarea_id, "aprobar")
            assert codigo == 200

            estado = db.tareas.find_one({"_id": tarea_id})
            assert estado["estado"] == "aprobada"
            assert estado["version"] == 4

            print("OK: aprobacion de tarea")

            # Una segunda aprobacion debe fallar
            codigo, _ = revisar(tarea_id, "aprobar")
            assert codigo == 409

            print("OK: aprobacion duplicada devuelve 409")


            # -----------------------------------------
            # Caso 2: rechazar una segunda tarea
            # -----------------------------------------

            otra_respuesta, otro_anotador = asignadas[1]

            otra_tarea_id = otra_respuesta["tarea_id"]
            otro_token = otra_respuesta["asignacion"]["token"]

            codigo = probar_envio(
                otra_tarea_id,
                otro_anotador,
                otro_token,
                geometria,
                etiqueta
            )
            assert codigo == 200

            codigo, _ = revisar(otra_tarea_id, "iniciar")
            assert codigo == 200

            # No debe permitirse rechazar sin motivo
            codigo, _ = revisar(otra_tarea_id, "rechazar")
            assert codigo == 422

            codigo, resultado = revisar(
                otra_tarea_id,
                "rechazar",
                "ETIQUETA_INCORRECTA"
            )
            assert codigo == 200
            assert resultado["estado_evento"] == "rechazada"

            tarea_rechazada = db.tareas.find_one({
                "_id": otra_tarea_id
            })

            assert tarea_rechazada["estado"] == "disponible"
            assert tarea_rechazada["version"] == 4
            assert tarea_rechazada["asignacion"] is None
            assert (
                tarea_rechazada["ultima_revision"]["codigo_motivo"]
                == "ETIQUETA_INCORRECTA"
            )

            print("OK: rechazo con motivo y retorno a disponible")


            # -----------------------------------------
            # Caso 3: exportar tareas aprobadas
            # -----------------------------------------

            codigo, exportacion = llamar_api(
                f"/proyectos/{proyecto_id}/exportar"
                "?pagina=1&limite=1",
                metodo="GET"
            )

            assert codigo == 200
            assert exportacion["total"] == 1
            assert len(exportacion["tareas"]) == 1
            assert exportacion["tareas"][0]["tarea_id"] == tarea_id

            # La segunda pagina debe estar vacia
            codigo, segunda_pagina = llamar_api(
                f"/proyectos/{proyecto_id}/exportar"
                "?pagina=2&limite=1",
                metodo="GET"
            )

            assert codigo == 200
            assert len(segunda_pagina["tareas"]) == 0

            # La tarea rechazada no puede exportarse
            ids_exportados = {
                tarea["tarea_id"]
                for tarea in exportacion["tareas"]
            }
            assert otra_tarea_id not in ids_exportados

            print("OK: exportacion paginada solo de aprobadas")
            print("OK: flujo operacional de prueba completado")



        finally:
            # Eliminar solamente los datos temporales
            db.tareas.delete_many({
                "dataset_id": conjunto
            })
            db.batches.delete_many({
                "dataset_id": conjunto
            })
            db.proyectos.delete_many({
                "dataset_id": conjunto
            })

            print("Conjunto temporal eliminado")


if __name__ == "__main__":
    main()
