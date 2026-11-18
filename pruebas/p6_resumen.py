import json
from pathlib import Path


usuarios_niveles = [10, 40, 100]

resultado = {}

for usuarios in usuarios_niveles:
    corridas = []

    for corrida in [1, 2, 3]:
        ruta = Path(
            f"evidence/p6_u{usuarios}_r{corrida}.json"
        )

        datos = json.loads(
            ruta.read_text()
        )

        corridas.append(datos)

    resumen = {
        "usuarios": usuarios,
        "corridas": 3,
        "envios_exitosos_total": sum(
            x["envios_exitosos"]
            for x in corridas
        ),
        "sin_tarea_total": sum(
            x["sin_tarea"]
            for x in corridas
        ),
        "envios_por_minuto_promedio": round(
            sum(
                x["envios_por_minuto"]
                for x in corridas
            ) / 3,
            3,
        ),
        "endpoints": {},
    }

    for endpoint in [
        "siguiente",
        "borrador",
        "enviar",
    ]:
        resumen["endpoints"][endpoint] = {
            "p50_ms_promedio": round(
                sum(
                    x["endpoints"][endpoint][
                        "p50_ms"
                    ]
                    for x in corridas
                ) / 3,
                3,
            ),
            "p95_ms_promedio": round(
                sum(
                    x["endpoints"][endpoint][
                        "p95_ms"
                    ]
                    for x in corridas
                ) / 3,
                3,
            ),
            "p99_ms_promedio": round(
                sum(
                    x["endpoints"][endpoint][
                        "p99_ms"
                    ]
                    for x in corridas
                ) / 3,
                3,
            ),
            "errores_total": sum(
                x["endpoints"][endpoint][
                    "errores"
                ]
                for x in corridas
            ),
            "muestras_total": sum(
                x["endpoints"][endpoint][
                    "muestras"
                ]
                for x in corridas
            ),
        }

    resultado[str(usuarios)] = resumen


salida = Path(
    "evidence/p6_resumen_final.json"
)

salida.write_text(
    json.dumps(
        resultado,
        indent=2,
        ensure_ascii=False,
    )
)

print(
    json.dumps(
        resultado,
        indent=2,
        ensure_ascii=False,
    )
)