import argparse
import asyncio
import json
import math
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import httpx


class Metricas:
    def __init__(self):
        self.latencias = defaultdict(list)
        self.estados = defaultdict(Counter)
        self.errores = Counter()
        self.excepciones = Counter()

        self.envios_exitosos = 0
        self.sin_tarea = 0

    def registrar(
        self,
        endpoint,
        latencia_ms,
        status=None,
        excepcion=None,
    ):
        self.latencias[endpoint].append(latencia_ms)

        if excepcion is not None:
            self.errores[endpoint] += 1
            self.excepciones[endpoint] += 1
            return

        self.estados[endpoint][str(status)] += 1

        if status is not None and status >= 400:
            self.errores[endpoint] += 1

        if endpoint == "siguiente" and status == 204:
            self.sin_tarea += 1

        if endpoint == "enviar" and status == 200:
            self.envios_exitosos += 1


def percentil(valores, p):
    if not valores:
        return None

    datos = sorted(valores)

    posicion = max(
        0,
        math.ceil((p / 100.0) * len(datos)) - 1,
    )

    return datos[posicion]


def resumen_endpoint(metricas, endpoint):
    datos = metricas.latencias.get(endpoint, [])

    if not datos:
        return {
            "muestras": 0,
            "p50_ms": None,
            "p95_ms": None,
            "p99_ms": None,
            "errores": metricas.errores[endpoint],
            "status": dict(metricas.estados[endpoint]),
        }

    return {
        "muestras": len(datos),
        "p50_ms": round(percentil(datos, 50), 3),
        "p95_ms": round(percentil(datos, 95), 3),
        "p99_ms": round(percentil(datos, 99), 3),
        "errores": metricas.errores[endpoint],
        "status": dict(metricas.estados[endpoint]),
    }


async def solicitar(
    cliente,
    metricas,
    endpoint,
    metodo,
    url,
    **kwargs,
):
    inicio = time.perf_counter()

    try:
        respuesta = await cliente.request(
            metodo,
            url,
            **kwargs,
        )

        latencia = (
            time.perf_counter() - inicio
        ) * 1000

        metricas.registrar(
            endpoint,
            latencia,
            status=respuesta.status_code,
        )

        return respuesta

    except httpx.HTTPError:
        latencia = (
            time.perf_counter() - inicio
        ) * 1000

        metricas.registrar(
            endpoint,
            latencia,
            excepcion=True,
        )

        return None


def anotacion_bbox(etiqueta):
    return {
        "geometria": "bbox",
        "etiquetas": [etiqueta],
        "elementos": [
            {
                "x": 120,
                "y": 90,
                "ancho": 240,
                "alto": 150,
            }
        ],
    }


async def usuario_warmup(
    cliente,
    gateway,
    proyecto_id,
    anotador_id,
    etiqueta,
    duracion,
    intervalo,
):
    metricas = Metricas()

    respuesta = await solicitar(
        cliente,
        metricas,
        "siguiente",
        "POST",
        f"{gateway}/tareas/siguiente",
        json={
            "proyecto_id": proyecto_id,
            "anotador_id": anotador_id,
        },
    )

    if respuesta is None or respuesta.status_code != 200:
        return

    tarea = respuesta.json()

    tarea_id = tarea["tarea_id"]
    token = tarea["token"]

    datos = anotacion_bbox(etiqueta)

    fin = time.monotonic() + duracion

    siguiente_borrador = time.monotonic() + intervalo

    while True:
        ahora = time.monotonic()

        if ahora >= fin:
            break

        espera = siguiente_borrador - ahora

        if espera > 0:
            await asyncio.sleep(espera)

        if time.monotonic() >= fin:
            break

        respuesta_borrador = await solicitar(
            cliente,
            metricas,
            "borrador",
            "PUT",
            f"{gateway}/tareas/{tarea_id}/borrador",
            json={
                "proyecto_id": proyecto_id,
                "anotador_id": anotador_id,
                "token": token,
                "datos": datos,
            },
        )

        if (
            respuesta_borrador is None
            or respuesta_borrador.status_code != 200
        ):
            break

        siguiente_borrador += intervalo

    # Warmup no debe consumir definitivamente
    # las tareas del dataset.
    await solicitar(
        cliente,
        metricas,
        "liberar",
        "POST",
        f"{gateway}/tareas/{tarea_id}/liberar",
        json={
            "proyecto_id": proyecto_id,
            "anotador_id": anotador_id,
            "token": token,
        },
    )


async def usuario_medicion(
    cliente,
    metricas,
    gateway,
    proyecto_id,
    anotador_id,
    etiqueta,
    fin,
    intervalo,
):
    respuesta = await solicitar(
        cliente,
        metricas,
        "siguiente",
        "POST",
        f"{gateway}/tareas/siguiente",
        json={
            "proyecto_id": proyecto_id,
            "anotador_id": anotador_id,
        },
    )

    if respuesta is None:
        return

    if respuesta.status_code != 200:
        return

    tarea = respuesta.json()

    tarea_id = tarea["tarea_id"]
    token = tarea["token"]

    datos = anotacion_bbox(etiqueta)

    siguiente_borrador = time.monotonic() + intervalo

    while True:
        ahora = time.monotonic()

        if ahora >= fin:
            break

        espera = siguiente_borrador - ahora

        if espera > 0:
            await asyncio.sleep(espera)

        if time.monotonic() >= fin:
            break

        respuesta_borrador = await solicitar(
            cliente,
            metricas,
            "borrador",
            "PUT",
            f"{gateway}/tareas/{tarea_id}/borrador",
            json={
                "proyecto_id": proyecto_id,
                "anotador_id": anotador_id,
                "token": token,
                "datos": datos,
            },
        )

        if (
            respuesta_borrador is None
            or respuesta_borrador.status_code != 200
        ):
            return

        siguiente_borrador += intervalo

    await solicitar(
        cliente,
        metricas,
        "enviar",
        "POST",
        f"{gateway}/tareas/{tarea_id}/enviar",
        json={
            "proyecto_id": proyecto_id,
            "anotador_id": anotador_id,
            "token": token,
            "anotacion": datos,
        },
    )


async def ejecutar(args):
    usuarios = [
        linea.strip()
        for linea in Path(
            args.usuarios
        ).read_text().splitlines()
        if linea.strip()
    ]

    if len(usuarios) < args.concurrencia:
        raise RuntimeError(
            f"Se requieren {args.concurrencia} "
            f"usuarios distintos y solo hay "
            f"{len(usuarios)}"
        )

    usuarios = usuarios[:args.concurrencia]

    limites = httpx.Limits(
        max_connections=args.concurrencia * 2,
        max_keepalive_connections=args.concurrencia,
    )

    timeout = httpx.Timeout(
        connect=5.0,
        read=15.0,
        write=15.0,
        pool=15.0,
    )

    async with httpx.AsyncClient(
        timeout=timeout,
        limits=limites,
    ) as cliente:

        # -----------------------------------
        # WARMUP
        # -----------------------------------

        if args.warmup > 0:
            await asyncio.gather(
                *[
                    usuario_warmup(
                        cliente,
                        args.gateway,
                        args.proyecto_id,
                        anotador_id,
                        args.etiqueta,
                        args.warmup,
                        args.intervalo,
                    )
                    for anotador_id in usuarios
                ]
            )

        # Pequeña separación entre fases.
        await asyncio.sleep(1)

        # -----------------------------------
        # MEDICIÓN
        # -----------------------------------

        metricas = Metricas()

        inicio_real = time.monotonic()
        inicio_utc = datetime.now(
            timezone.utc
        ).isoformat()

        fin = inicio_real + args.duracion

        await asyncio.gather(
            *[
                usuario_medicion(
                    cliente,
                    metricas,
                    args.gateway,
                    args.proyecto_id,
                    anotador_id,
                    args.etiqueta,
                    fin,
                    args.intervalo,
                )
                for anotador_id in usuarios
            ]
        )

        duracion_real = (
            time.monotonic() - inicio_real
        )

    endpoints = {}

    for endpoint in [
        "siguiente",
        "borrador",
        "enviar",
    ]:
        endpoints[endpoint] = resumen_endpoint(
            metricas,
            endpoint,
        )

    envios_por_minuto = (
        metricas.envios_exitosos
        / duracion_real
        * 60
        if duracion_real > 0
        else 0
    )

    resultado = {
        "inicio_utc": inicio_utc,
        "usuarios": args.concurrencia,
        "warmup_s": args.warmup,
        "duracion_objetivo_s": args.duracion,
        "duracion_real_s": round(
            duracion_real,
            3,
        ),
        "intervalo_borrador_s": args.intervalo,
        "envios_exitosos": (
            metricas.envios_exitosos
        ),
        "envios_por_minuto": round(
            envios_por_minuto,
            3,
        ),
        "sin_tarea": metricas.sin_tarea,
        "endpoints": endpoints,
    }

    print(json.dumps(
        resultado,
        indent=2,
        ensure_ascii=False,
    ))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--gateway",
        default="http://gateway:8000",
    )

    parser.add_argument(
        "--proyecto-id",
        required=True,
    )

    parser.add_argument(
        "--usuarios",
        default="/app/usuarios_p6.txt",
    )

    parser.add_argument(
        "--etiqueta",
        default="imagen#vehiculo",
    )

    parser.add_argument(
        "--concurrencia",
        type=int,
        required=True,
    )

    parser.add_argument(
        "--warmup",
        type=float,
        default=30,
    )

    parser.add_argument(
        "--duracion",
        type=float,
        default=90,
    )

    parser.add_argument(
        "--intervalo",
        type=float,
        default=2,
    )

    args = parser.parse_args()

    asyncio.run(ejecutar(args))


if __name__ == "__main__":
    main()