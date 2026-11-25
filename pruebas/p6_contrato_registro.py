import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

BASE = os.getenv(
    "REGISTRO_URL",
    "http://registro:8001"
)

EVIDENCE = Path("/app/evidence")

RUTAS_ESPERADAS = {
    ("GET", "/salud"),
    ("POST", "/tareas/siguiente"),
    ("POST", "/tareas/{tarea_id}/enviar"),
    ("POST", "/tareas/{tarea_id}/revision"),
    ("GET", "/proyectos/{proyecto_id}/exportar"),
    ("POST", "/comandos"),
}

METODOS = {
    "get", "post", "put",
    "patch", "delete"
}


def obtener_json(ruta):
    with urlopen(
        f"{BASE}{ruta}",
        timeout=15
    ) as respuesta:
        assert respuesta.status == 200
        return json.loads(
            respuesta.read().decode("utf-8")
        )


def main():
    EVIDENCE.mkdir(
        parents=True,
        exist_ok=True
    )

    # 1. Comprobar salud del registro
    salud = obtener_json("/salud")

    assert salud["estado"] == "operativo"
    assert salud["mongodb"] == "disponible"

    print("OK: servicio REST y MongoDB disponibles")

    # 2. Obtener el contrato OpenAPI
    contrato = obtener_json("/openapi.json")

    rutas_actuales = set()

    for ruta, operaciones in contrato["paths"].items():
        for metodo in operaciones:
            if metodo.lower() in METODOS:
                rutas_actuales.add(
                    (metodo.upper(), ruta)
                )

    print(
        f"Rutas REST internas: "
        f"{len(rutas_actuales)}"
    )

    # 3. Comparar con las seis rutas esperadas
    faltantes = (
        RUTAS_ESPERADAS - rutas_actuales
    )

    adicionales = (
        rutas_actuales - RUTAS_ESPERADAS
    )

    if faltantes:
        print("Rutas faltantes:")
        for ruta in sorted(faltantes):
            print(" ", ruta)

    if adicionales:
        print("Rutas adicionales:")
        for ruta in sorted(adicionales):
            print(" ", ruta)

    assert not faltantes
    assert not adicionales
    assert len(rutas_actuales) == 6

    for metodo, ruta in sorted(rutas_actuales):
        print(f"OK: {metodo} {ruta}")

    # 4. Guardar el contrato completo
    ruta_openapi = (
        EVIDENCE / "p6-registro-openapi.json"
    )

    ruta_openapi.write_text(
        json.dumps(
            contrato,
            indent=2,
            ensure_ascii=False
        ),
        encoding="utf-8"
    )

    # 5. Guardar resumen verificable
    resumen = {
        "fecha": datetime.now(
            timezone.utc
        ).isoformat(),
        "servicio": "registro",
        "estado": "validado",
        "total_rutas": len(rutas_actuales),
        "rutas": [
            {
                "metodo": metodo,
                "ruta": ruta
            }
            for metodo, ruta
            in sorted(rutas_actuales)
        ]
    }

    (EVIDENCE / "p6-contrato-registro.json").write_text(
        json.dumps(
            resumen,
            indent=2,
            ensure_ascii=False
        ),
        encoding="utf-8"
    )

    print("OK: seis rutas internas verificadas")
    print("OK: contrato OpenAPI exportado")


if __name__ == "__main__":
    main()