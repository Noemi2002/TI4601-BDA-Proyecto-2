import argparse
import json
import sys
import time

import httpx


def imprimir(nombre: str, respuesta: httpx.Response) -> None:
    print(f"\n[{nombre}] HTTP {respuesta.status_code}")

    if respuesta.content:
        try:
            print(json.dumps(
                respuesta.json(),
                indent=2,
                ensure_ascii=False,
            ))
        except ValueError:
            print(respuesta.text)


def anotacion_bbox(etiqueta: str) -> dict:
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


def ejecutar_anotador(args) -> int:
    gateway = args.gateway.rstrip("/")

    with httpx.Client(timeout=10.0) as cliente:

        # 1. Solicitar tarea
        inicio = time.perf_counter()

        respuesta = cliente.post(
            f"{gateway}/tareas/siguiente",
            json={
                "proyecto_id": args.proyecto_id,
                "anotador_id": args.anotador_id,
            },
        )

        imprimir("siguiente", respuesta)

        if respuesta.status_code == 204:
            print("No hay tareas disponibles.")
            return 0

        if respuesta.status_code != 200:
            return 1

        tarea = respuesta.json()

        tarea_id = tarea["tarea_id"]
        token = tarea["token"]

        print(f"\nTarea: {tarea_id}")
        print(f"Token: {token}")

        # Por ahora probaremos el proyecto imagen/bbox.
        datos = anotacion_bbox(args.etiqueta)

        # 2. Edición + borrador cada 2 segundos
        transcurrido = 0.0

        while transcurrido < args.duracion_edicion:
            time.sleep(args.intervalo_borrador)

            respuesta_borrador = cliente.put(
                f"{gateway}/tareas/{tarea_id}/borrador",
                json={
                    "proyecto_id": args.proyecto_id,
                    "anotador_id": args.anotador_id,
                    "token": token,
                    "datos": datos,
                },
            )

            imprimir("borrador", respuesta_borrador)

            if respuesta_borrador.status_code != 200:
                return 1

            transcurrido = time.perf_counter() - inicio

        # Permite simular P3:
        # usuario desaparece sin enviar ni liberar.
        if args.interrumpir:
            print(
                "\nINTERRUPCIÓN SIMULADA: "
                "no se envía ni se libera la tarea."
            )
            return 0

        # 3. Enviar anotación definitiva
        respuesta_envio = cliente.post(
            f"{gateway}/tareas/{tarea_id}/enviar",
            json={
                "proyecto_id": args.proyecto_id,
                "anotador_id": args.anotador_id,
                "token": token,
                "anotacion": datos,
            },
        )

        imprimir("enviar", respuesta_envio)

        if respuesta_envio.status_code != 200:
            return 1

        # 4. Revisión opcional
        if args.revisor_id:
            respuesta_revision = cliente.post(
                f"{gateway}/tareas/{tarea_id}/revision",
                json={
                    "revisor_id": args.revisor_id,
                    "accion": "iniciar",
                },
            )

            imprimir("revision iniciar", respuesta_revision)

            if respuesta_revision.status_code != 200:
                return 1

            payload_revision = {
                "revisor_id": args.revisor_id,
                "accion": args.decision,
            }

            if args.decision == "rechazar":
                payload_revision["codigo_motivo"] = (
                    args.codigo_motivo
                )

            respuesta_revision = cliente.post(
                f"{gateway}/tareas/{tarea_id}/revision",
                json=payload_revision,
            )

            imprimir(
                f"revision {args.decision}",
                respuesta_revision,
            )

            if respuesta_revision.status_code != 200:
                return 1

        print("\nSimulación finalizada correctamente.")
        return 0


def recuperar(args) -> int:
    gateway = args.gateway.rstrip("/")

    with httpx.Client(timeout=10.0) as cliente:
        inicio = time.perf_counter()

        respuesta = cliente.post(
            f"{gateway}/sesiones/recuperar",
            json={
                "anotador_id": args.anotador_id,
            },
        )

        latencia_ms = (
            time.perf_counter() - inicio
        ) * 1000

        imprimir("recuperar", respuesta)

        print(
            f"\nLatencia recuperación: "
            f"{latencia_ms:.2f} ms"
        )

        return 0 if respuesta.status_code == 200 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Simulador HTTP de la plataforma",
    )

    parser.add_argument(
        "--gateway",
        default="http://gateway:8000",
    )

    subparsers = parser.add_subparsers(
        dest="comando",
        required=True,
    )

    anotador = subparsers.add_parser("anotar")

    anotador.add_argument(
        "--proyecto-id",
        required=True,
    )

    anotador.add_argument(
        "--anotador-id",
        required=True,
    )

    anotador.add_argument(
        "--etiqueta",
        required=True,
    )

    anotador.add_argument(
        "--duracion-edicion",
        type=float,
        default=6.0,
    )

    anotador.add_argument(
        "--intervalo-borrador",
        type=float,
        default=2.0,
    )

    anotador.add_argument(
        "--interrumpir",
        action="store_true",
    )

    anotador.add_argument(
        "--revisor-id",
    )

    anotador.add_argument(
        "--decision",
        choices=["aprobar", "rechazar"],
        default="aprobar",
    )

    anotador.add_argument(
        "--codigo-motivo",
        default="ETIQUETA_INCORRECTA",
    )

    anotador.set_defaults(func=ejecutar_anotador)

    recuperacion = subparsers.add_parser(
        "recuperar",
    )

    recuperacion.add_argument(
        "--anotador-id",
        required=True,
    )

    recuperacion.set_defaults(func=recuperar)

    args = parser.parse_args()

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())