import copy
import json
import os

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from bson import json_util
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern


EVIDENCE = Path("/app/evidence")
URI = os.environ["MONGO_URI"]

INDICES = {
    "tareas": {
        "idx_tareas_proyecto_estado_prioridad": [
            ("proyecto_id", 1),
            ("estado", 1),
            ("prioridad", -1),
        ],
        "idx_tareas_anotador_estado": [
            ("asignacion.anotador_id", 1),
            ("estado", 1),
        ],
        "idx_tareas_batch": [
            ("batch_id", 1),
        ],
    },
    "proyectos": {
        "idx_proyectos_cliente_estado": [
            ("cliente_id", 1),
            ("estado", 1),
        ],
    },
}


def verificar_indices(db):
    print("Verificando los cuatro indices obligatorios")

    for coleccion, indices in INDICES.items():
        existentes = {
            indice["name"]: list(indice["key"].items())
            for indice in db[coleccion].list_indexes()
        }

        for nombre, campos in indices.items():
            assert nombre in existentes, (
                f"Falta el indice: {nombre}"
            )

            assert existentes[nombre] == campos, (
                f"Campos incorrectos en {nombre}: "
                f"{existentes[nombre]}"
            )

            print(f"OK: {nombre}")

    print()


def buscar_indices_en_plan(objeto):
    """
    Extrae los nombres de indices del plan.
    Funciona con planes clasicos y SBE.
    """
    encontrados = set()

    if isinstance(objeto, dict):
        nombre = objeto.get("indexName")

        if isinstance(nombre, str):
            encontrados.add(nombre)

        for valor in objeto.values():
            encontrados.update(
                buscar_indices_en_plan(valor)
            )

    elif isinstance(objeto, list):
        for valor in objeto:
            encontrados.update(
                buscar_indices_en_plan(valor)
            )

    return encontrados


def ejecutar_explain(
    db,
    coleccion,
    filtro,
    orden=None,
    hint=None
):
    comando = {
        "find": coleccion,
        "filter": filtro,
    }

    if orden:
        comando["sort"] = orden

    if hint is not None:
        comando["hint"] = hint

    explicacion = db.command({
        "explain": comando,
        "verbosity": "executionStats"
    })

    estadisticas = explicacion["executionStats"]

    plan = explicacion["queryPlanner"]["winningPlan"]

    resumen = {
        "devueltos": estadisticas["nReturned"],
        "claves_examinadas": (
            estadisticas["totalKeysExamined"]
        ),
        "documentos_examinados": (
            estadisticas["totalDocsExamined"]
        ),
        "tiempo_ms": (
            estadisticas["executionTimeMillis"]
        ),
        "indices_usados": sorted(
            buscar_indices_en_plan(plan)
        ),
    }

    return resumen, explicacion


def mostrar_resultado(nombre, tipo, resumen):
    print(f"  {tipo}:")

    print(
        f"    Devueltos: "
        f"{resumen['devueltos']}"
    )

    print(
        f"    Claves examinadas: "
        f"{resumen['claves_examinadas']}"
    )

    print(
        f"    Documentos examinados: "
        f"{resumen['documentos_examinados']}"
    )

    print(
        f"    Tiempo reportado: "
        f"{resumen['tiempo_ms']} ms"
    )

    print(
        "    Indices: "
        f"{resumen['indices_usados']}"
    )


def evaluar_consulta(db, caso):
    print("=" * 60)
    print(caso["nombre"])
    print("=" * 60)

    modos = {
        "automatico": None,
        "indice_forzado": caso["indice"],
        "sin_indice": {"$natural": 1},
    }

    resumen_caso = {
        "coleccion": caso["coleccion"],
        "filtro": caso["filtro"],
        "orden": caso.get("orden"),
        "indice_esperado": caso["indice"],
        "resultados": {},
    }

    explicaciones = {}

    for tipo, hint in modos.items():
        resumen, explicacion = ejecutar_explain(
            db=db,
            coleccion=caso["coleccion"],
            filtro=caso["filtro"],
            orden=caso.get("orden"),
            hint=hint
        )

        resumen_caso["resultados"][tipo] = resumen
        explicaciones[tipo] = explicacion

        mostrar_resultado(
            caso["nombre"],
            tipo,
            resumen
        )

    automatico = resumen_caso["resultados"]["automatico"]
    forzado = resumen_caso["resultados"]["indice_forzado"]
    natural = resumen_caso["resultados"]["sin_indice"]

    # Verificar que el indice funciona
    assert caso["indice"] in forzado["indices_usados"], (
        f"El indice {caso['indice']} no se utilizo "
        "en el plan forzado"
    )

    # Las tres modalidades deben recuperar
    # la misma cantidad de documentos.
    assert (
        automatico["devueltos"]
        == forzado["devueltos"]
        == natural["devueltos"]
    ), "Los planes devolvieron cantidades diferentes"

    assert forzado["devueltos"] > 0, (
        "La consulta no encontro documentos"
    )

    assert natural["claves_examinadas"] == 0, (
        "El plan sin indice examino claves"
    )

    if caso["indice"] in automatico["indices_usados"]:
        print("OK: MongoDB eligio el indice esperado")
    else:
        print(
            "AVISO: MongoDB eligio otro plan automaticamente"
        )

    print("OK: indice comprobado mediante hint")
    print()

    return resumen_caso, explicaciones


def main():
    EVIDENCE.mkdir(
        parents=True,
        exist_ok=True
    )

    identificador = str(uuid4())
    dataset_temporal = f"p4-indices-{identificador}"

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

        # 1. Verificar las definiciones de indices
        verificar_indices(db)

        # 2. Obtener documentos reales
        tarea_base = db.tareas.find_one({
            "dataset_id": "principal-v1",
            "estado": "disponible"
        })

        assert tarea_base is not None, (
            "No se encontraron tareas del dataset principal"
        )

        proyecto = db.proyectos.find_one({
            "_id": tarea_base["proyecto_id"]
        })

        assert proyecto is not None

        anotador = db.anotadores.find_one({
            "dataset_id": "principal-v1",
            "estado": "activo",
            "rol": "anotador"
        })

        assert anotador is not None

        # 3. Crear documentos temporales para comprobar
        #    el indice de asignaciones por anotador.
        ahora = datetime.now(timezone.utc)

        documentos_temporales = []

        for _ in range(12):
            tarea = copy.deepcopy(tarea_base)

            tarea["_id"] = str(uuid4())
            tarea["dataset_id"] = dataset_temporal
            tarea["estado"] = "abierta"
            tarea["version"] = 1

            tarea["asignacion"] = {
                "anotador_id": anotador["_id"],
                "token": str(uuid4()),
                "asignada_en": ahora,
                "vence_en": ahora + timedelta(
                    minutes=10
                )
            }

            tarea["creado_en"] = ahora
            tarea["actualizado_en"] = ahora

            documentos_temporales.append(tarea)

        try:
            db.tareas.insert_many(
                documentos_temporales
            )

            print(
                "OK: 12 documentos temporales creados"
            )
            print()

            # 4. Consultas que corresponden
            #    a los cuatro indices obligatorios
            casos = [
                {
                    "nombre": (
                        "Consulta 1: tareas por "
                        "proyecto, estado y prioridad"
                    ),
                    "coleccion": "tareas",
                    "filtro": {
                        "proyecto_id": (
                            tarea_base["proyecto_id"]
                        ),
                        "estado": "disponible"
                    },
                    "orden": {
                        "prioridad": -1
                    },
                    "indice": (
                        "idx_tareas_proyecto_estado_prioridad"
                    ),
                },
                {
                    "nombre": (
                        "Consulta 2: tareas "
                        "por anotador y estado"
                    ),
                    "coleccion": "tareas",
                    "filtro": {
                        "asignacion.anotador_id": (
                            anotador["_id"]
                        ),
                        "estado": "abierta"
                    },
                    "indice": (
                        "idx_tareas_anotador_estado"
                    ),
                },
                {
                    "nombre": (
                        "Consulta 3: tareas por lote"
                    ),
                    "coleccion": "tareas",
                    "filtro": {
                        "batch_id": (
                            tarea_base["batch_id"]
                        )
                    },
                    "indice": (
                        "idx_tareas_batch"
                    ),
                },
                {
                    "nombre": (
                        "Consulta 4: proyectos "
                        "por cliente y estado"
                    ),
                    "coleccion": "proyectos",
                    "filtro": {
                        "cliente_id": (
                            proyecto["cliente_id"]
                        ),
                        "estado": "activo"
                    },
                    "indice": (
                        "idx_proyectos_cliente_estado"
                    ),
                },
            ]

            resultados = []
            planes_completos = {}

            for caso in casos:
                resumen, planes = evaluar_consulta(
                    db,
                    caso
                )

                resultados.append(resumen)
                planes_completos[
                    caso["nombre"]
                ] = planes

            # 5. Guardar resultados del experimento
            ruta_resumen = (
                EVIDENCE / "p4_indices_resumen.json"
            )

            ruta_explain = (
                EVIDENCE / "p4_indices_explain.json"
            )

            with ruta_resumen.open(
                "w",
                encoding="utf-8"
            ) as archivo:
                json.dump(
                    resultados,
                    archivo,
                    indent=2,
                    ensure_ascii=False,
                    default=str
                )

            with ruta_explain.open(
                "w",
                encoding="utf-8"
            ) as archivo:
                archivo.write(
                    json_util.dumps(
                        planes_completos,
                        indent=2,
                        ensure_ascii=False
                    )
                )

            print("=" * 60)
            print("OK: cuatro indices evaluados")
            print(
                "Resumen guardado:",
                ruta_resumen
            )
            print(
                "Planes completos:",
                ruta_explain
            )

        finally:
            # Eliminar solamente las tareas
            # creadas para este experimento.
            resultado = db.tareas.delete_many({
                "dataset_id": dataset_temporal
            })

            print(
                "Documentos temporales eliminados:",
                resultado.deleted_count
            )


if __name__ == "__main__":
    main()