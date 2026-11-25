
from datetime import datetime
from pathlib import Path
import hashlib
import json
import os

from pymongo import MongoClient, UpdateOne
from pymongo.write_concern import WriteConcern

DATOS = Path(__file__).resolve().parent.parent / 'datos'
URI = os.environ['MONGO_URI']

ORDEN = [
    'clientes',
    'proyectos',
    'batches',
    'anotadores',
    'tareas'
]

LOTE = 500


def convertir_fechas(objeto):

    if isinstance(objeto, list):
        return [
            convertir_fechas(elemento)
            for elemento in objeto
        ]

    if isinstance(objeto, dict):
        return {
            clave: (
                datetime.fromisoformat(valor)
                if isinstance(valor, str)
                and (
                    clave.endswith('_en')
                    or clave in ('fecha_limite', 'vence_en')
                )
                else convertir_fechas(valor)
            )
            for clave, valor in objeto.items()
        }

    return objeto


def validar_archivo(ruta, esperado):

    with ruta.open('rb') as archivo:
        sha = hashlib.file_digest(
            archivo, 'sha256'
        ).hexdigest()

    if sha != esperado:
        raise RuntimeError(
            f'Hash incorrecto: {ruta}'
        )


def cargar_coleccion(db, nombre, conjunto, esperado):

    coleccion = db.get_collection(
        nombre,
        write_concern=WriteConcern(
            w='majority',
            wtimeout=15000
        )
    )

    operaciones = []
    insertados = 0
    procesados = 0

    with (
        DATOS / f'{nombre}.jsonl'
    ).open(encoding='utf-8') as archivo:

        for linea in archivo:

            doc = convertir_fechas(
                json.loads(linea)
            )

            if doc['dataset_id'] != conjunto:
                raise RuntimeError(
                    f'Dataset incorrecto en {nombre}'
                )

            operaciones.append(
                UpdateOne(
                    {'_id': doc['_id']},
                    {'$setOnInsert': doc},
                    upsert=True
                )
            )

            procesados += 1

            if len(operaciones) == LOTE:
                resultado = coleccion.bulk_write(
                    operaciones,
                    ordered=False
                )

                insertados += resultado.upserted_count
                operaciones.clear()

        if operaciones:
            resultado = coleccion.bulk_write(
                operaciones,
                ordered=False
            )
            insertados += resultado.upserted_count

    if procesados != esperado:
        raise RuntimeError(
            f'{nombre}: archivo contiene '
            f'{procesados}, se esperaban {esperado}'
        )

    total = coleccion.count_documents({
        'dataset_id': conjunto
    })

    if total != esperado:
        raise RuntimeError(
            f'{nombre}: MongoDB tiene '
            f'{total}, se esperaban {esperado}'
        )

    print(
        f'OK {nombre}: {total} documentos '
        f'del conjunto ({insertados} nuevos)'
    )


def main():

    manifiesto = json.loads(
        (DATOS / 'manifest.json').read_text(
            encoding='utf-8'
        )
    )

    for nombre in ORDEN:
        validar_archivo(
            DATOS / f'{nombre}.jsonl',
            manifiesto['archivos'][nombre]['sha256']
        )

    with MongoClient(
        URI,
        serverSelectionTimeoutMS=10000,
        retryWrites=True
    ) as cliente:

        cliente.admin.command('ping')
        db = cliente['plataforma']

        for nombre in ORDEN:
            cargar_coleccion(
                db,
                nombre,
                manifiesto['dataset_id'],
                manifiesto['archivos'][nombre]['cantidad']
            )

    print(
        'OK: carga completa con w=majority, '
        'sin eliminar documentos existentes.'
    )


if __name__ == '__main__':
    main()
