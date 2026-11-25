import csv
import json
import re
from collections import defaultdict
from pathlib import Path


NIVELES = [10, 40, 100]
CORRIDAS = [1, 2, 3]


def parse_porcentaje(valor):
    return float(valor.replace("%", "").strip())


def memoria_a_mib(valor):
    """
    Convierte valores como:
    98.88MiB
    1.2GiB
    850KiB
    """
    valor = valor.strip()

    match = re.match(
        r"([0-9.]+)\s*(KiB|MiB|GiB|B)",
        valor
    )

    if not match:
        return 0.0

    cantidad = float(match.group(1))
    unidad = match.group(2)

    if unidad == "GiB":
        return cantidad * 1024

    if unidad == "MiB":
        return cantidad

    if unidad == "KiB":
        return cantidad / 1024

    if unidad == "B":
        return cantidad / (1024 * 1024)

    return 0.0


resultado = {}


for usuarios in NIVELES:
    servicios = defaultdict(
        lambda: {
            "cpu": [],
            "memoria_mib": [],
        }
    )

    for corrida in CORRIDAS:
        ruta = Path(
            f"evidence/"
            f"p6_u{usuarios}_r{corrida}_docker_stats.csv"
        )

        with ruta.open(
            newline="",
            encoding="utf-8"
        ) as archivo:

            lector = csv.DictReader(archivo)

            for fila in lector:
                nombre = fila["nombre"]

                cpu = parse_porcentaje(
                    fila["cpu"]
                )

                memoria_usada = (
                    fila["memoria"].split("/")[0]
                )

                memoria = memoria_a_mib(
                    memoria_usada
                )

                servicios[nombre]["cpu"].append(cpu)

                servicios[nombre][
                    "memoria_mib"
                ].append(memoria)

    resumen_servicios = {}

    for nombre, datos in servicios.items():

        cpu = datos["cpu"]
        memoria = datos["memoria_mib"]

        resumen_servicios[nombre] = {
            "muestras": len(cpu),

            "cpu_promedio_pct": round(
                sum(cpu) / len(cpu),
                3,
            ),

            "cpu_max_pct": round(
                max(cpu),
                3,
            ),

            "memoria_promedio_mib": round(
                sum(memoria) / len(memoria),
                3,
            ),

            "memoria_max_mib": round(
                max(memoria),
                3,
            ),
        }

    resultado[str(usuarios)] = resumen_servicios


salida = Path(
    "evidence/p6_recursos_resumen.json"
)

salida.write_text(
    json.dumps(
        resultado,
        indent=2,
        ensure_ascii=False,
    ),
    encoding="utf-8",
)

print(
    json.dumps(
        resultado,
        indent=2,
        ensure_ascii=False,
    )
)