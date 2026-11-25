
from pathlib import Path
from datetime import datetime, timedelta, timezone
import hashlib
import json
import random
import uuid

SALIDA = Path(__file__).resolve().parent.parent / 'datos'
CONJUNTO = 'principal-v1'
SEMILLA = 4601
BASE = datetime(2026, 11, 1, tzinfo=timezone.utc)
RNG = random.Random(SEMILLA)

MODALIDADES = ['texto'] * 36 + ['imagen'] * 36 + ['video'] * 24 + ['lidar'] * 24
RNG.shuffle(MODALIDADES)

GEOMETRIA = {
    'texto': 'span',
    'imagen': 'bbox',
    'video': 'bbox_video',
    'lidar': 'cuboide_3d'
}

TIPO = {
    'texto': 'clasificacion_texto',
    'imagen': 'deteccion_objetos',
    'video': 'seguimiento',
    'lidar': 'deteccion_3d'
}

MIME = {
    'texto': 'text/plain',
    'imagen': 'image/jpeg',
    'video': 'video/mp4',
    'lidar': 'application/octet-stream'
}

TAXONOMIAS = {
    'texto': [
        {
            'etiqueta': 'texto#positivo',
            'instruccion': 'Identificar contenido positivo.'
        },
        {
            'etiqueta': 'texto#negativo',
            'instruccion': 'Identificar contenido negativo.'
        }
    ],
    'imagen': [
        {
            'etiqueta': 'imagen#vehiculo',
            'instruccion': 'Marcar vehiculos con cajas.'
        }
    ],
    'video': [
        {
            'etiqueta': 'video#vehiculo',
            'instruccion': 'Seguir vehiculos entre cuadros.'
        }
    ],
    'lidar': [
        {
            'etiqueta': 'lidar#peaton',
            'instruccion': 'Marcar personas a pie.'
        },
        {
            'etiqueta': 'lidar#vehiculo',
            'instruccion': 'Marcar vehiculos.'
        },
        {
            'etiqueta': 'lidar#ciclista',
            'instruccion': 'Incluir ciclistas y skaters.'
        },
        {
            'etiqueta': 'lidar#signal',
            'instruccion': 'Identificar senales de transito.'
        }
    ]
}


def uid(tipo, numero):
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f'ti4601:{CONJUNTO}:{tipo}:{numero}'
        )
    )


def fecha(desplazamiento=0):
    return (
        BASE + timedelta(minutes=desplazamiento)
    ).isoformat()


def generar_clientes():
    for i in range(1, 61):
        yield {
            '_id': uid('cliente', i),
            'dataset_id': CONJUNTO,
            'nombre': f'Cliente sintetico {i:03d}',
            'email': f'cliente{i:03d}@example.org',
            'creado_en': fecha(i)
        }


def generar_proyectos():
    for i, modalidad in enumerate(MODALIDADES, start=1):
        yield {
            '_id': uid('proyecto', i),
            'dataset_id': CONJUNTO,
            'cliente_id': uid('cliente', (i - 1) // 2 + 1),
            'nombre': f'Proyecto {modalidad} {i:03d}',
            'estado': 'activo',
            'modalidad': modalidad,
            'geometria': GEOMETRIA[modalidad],
            'taxonomia': TAXONOMIAS[modalidad],
            'prioridad_defecto': 2,
            'sla_horas': 48,
            'fecha_limite': fecha(150_000),
            'creado_en': fecha(i)
        }


def generar_batches():
    for i in range(1, 601):
        yield {
            '_id': uid('batch', i),
            'dataset_id': CONJUNTO,
            'proyecto_id': uid('proyecto', (i - 1) // 5 + 1),
            'nombre': f'Lote {i:04d}',
            'descripcion': 'Lote de datos sinteticos',
            'estado': 'activo',
            'creado_en': fecha(i)
        }


def generar_anotadores():
    for i in range(1, 301):
        yield {
            '_id': uid('anotador', i),
            'dataset_id': CONJUNTO,
            'nombre': f'Usuario sintetico {i:03d}',
            'email': f'usuario{i:03d}@example.org',
            'equipo_id': uid('equipo', (i - 1) // 10 + 1),
            'rol': 'revisor' if i % 10 == 0 else 'anotador',
            'estado': 'activo',
            'creado_en': fecha(i)
        }


def generar_tareas():
    for i in range(1, 60_001):

        batch_n = (i - 1) // 100 + 1
        proyecto_n = (batch_n - 1) // 5 + 1

        modalidad = MODALIDADES[proyecto_n - 1]

        uri = (
            f's3://dataset-sintetico/'
            f'{modalidad}/archivo-{i:06d}'
        )

        yield {
            '_id': uid('tarea', i),
            'dataset_id': CONJUNTO,

            'proyecto_id': uid('proyecto', proyecto_n),
            'batch_id': uid('batch', batch_n),

            'modalidad': modalidad,
            'tipo': TIPO[modalidad],
            'geometria': GEOMETRIA[modalidad],

            'archivo': {
                'uri': uri,
                'sha256': hashlib.sha256(
                    f'contenido-sintetico:{i}'.encode()
                ).hexdigest(),
                'bytes': RNG.randint(5_000, 5_000_000),
                'mime': MIME[modalidad],
                'meta': {
                    'valor_min': 0,
                    'valor_max': 100
                }
            },

            'estado': 'disponible',
            'prioridad': RNG.randint(1, 5),

            'asignacion': None,
            'anotacion_vigente': None,
            'ultima_revision': None,
            'ultimo_evento_id': None,

            'version': 0,
            'creado_en': fecha(i // 100),
            'actualizado_en': fecha(i // 100)
        }


def guardar(nombre, documentos):

    ruta = SALIDA / f'{nombre}.jsonl'
    digest = hashlib.sha256()
    cantidad = 0

    with ruta.open('wb') as salida:
        for documento in documentos:

            linea = (
                json.dumps(
                    documento,
                    ensure_ascii=False,
                    separators=(',', ':')
                ) + '\n'
            ).encode('utf-8')

            salida.write(linea)
            digest.update(linea)
            cantidad += 1

    print(f'{nombre}: {cantidad} documentos')

    return {
        'cantidad': cantidad,
        'sha256': digest.hexdigest()
    }


def main():

    SALIDA.mkdir(parents=True, exist_ok=True)

    archivos = {
        'clientes': guardar(
            'clientes', generar_clientes()
        ),
        'proyectos': guardar(
            'proyectos', generar_proyectos()
        ),
        'batches': guardar(
            'batches', generar_batches()
        ),
        'anotadores': guardar(
            'anotadores', generar_anotadores()
        ),
        'tareas': guardar(
            'tareas', generar_tareas()
        ),
    }

    assert {
        k: v['cantidad']
        for k, v in archivos.items()
    } == {
        'clientes': 60,
        'proyectos': 120,
        'batches': 600,
        'anotadores': 300,
        'tareas': 60000,
    }

    manifiesto = {
        'dataset_id': CONJUNTO,
        'semilla': SEMILLA,
        'archivos': archivos
    }

    (SALIDA / 'manifest.json').write_text(
        json.dumps(manifiesto, indent=2) + '\n',
        encoding='utf-8'
    )

    print(
        'OK: 61 080 documentos generados. '
        'Manifiesto creado.'
    )


if __name__ == '__main__':
    main()
