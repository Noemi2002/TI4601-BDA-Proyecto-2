"""P5 - API de analitica a traves del Gateway (GET /analitica/{q1..q5}).

Se ejecuta en el host, contra el unico puerto publicado:

  python3 pruebas/p5_analitica_api.py 2>&1 | tee evidence/p5-analitica-api.txt

Comprueba que cada consulta responde por el Gateway, informa el costo de
arranque de Spark (solo la primera) y los tiempos posteriores, y que los codigos
de error son los esperados. Si existen los CSV de datalake/resultados/qN (de
`consultas.py`), compara la cantidad de filas.
"""
import glob
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("GATEWAY_URL", "http://localhost:8000").rstrip("/")
fallos = []
REF_Q1 = os.environ.get("REF_Q1", "2026-11-23 23:58:45")

def ok(cond, msg):
    print(("OK:    " if cond else "FALLA: ") + msg)
    if not cond:
        fallos.append(msg)


def get(ruta):
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(BASE + ruta, timeout=180) as r:
            return r.status, json.loads(r.read()), time.perf_counter() - t0
    except urllib.error.HTTPError as e:
        try:
            cuerpo = json.loads(e.read())
        except ValueError:
            cuerpo = {}
        return e.code, cuerpo, time.perf_counter() - t0


print(f"Gateway: {BASE}\n")
st, salud, _ = get("/salud")
print(f"/salud -> {st} {json.dumps(salud)[:300]}\n")

print(f"{'consulta':8} {'HTTP':5} {'filas':6} {'spark_s':8} {'arranque_s':10} {'total_s':8}")
for q in ["q1", "q2", "q3", "q4", "q5"]:
    for rep in range(3):
        #st, d, total = get(f"/analitica/{q}")
        ruta = f"/analitica/{q}" + (f"?ref={REF_Q1.replace(' ', '%20')}" if q == "q1" and REF_Q1 else "")
        st, d, total = get(ruta)
        if st != 200:
            ok(False, f"{q}: HTTP {st} {d}")
            break
        print(f"{q:8} {st:<5} {d['filas']:<6} {d['tiempo_s']:<8} {d['arranque_spark_s']:<10} {total:.3f}")
        if rep == 0:
            csvs = glob.glob(f"datalake/resultados/{q}/part-*.csv")
            if csvs:
                n = sum(1 for _ in open(csvs[0], encoding="utf-8")) - 1
                ok(n == d["filas"], f"{q}: {d['filas']} filas por la API y {n} en el CSV de spark-submit")
            else:
                ok(d["filas"] > 0, f"{q}: devuelve {d['filas']} filas")

st, d, _ = get("/analitica/q9")
ok(st == 404, f"consulta inexistente -> {st} (se espera 404)")
st, d, _ = get("/analitica/q1?conjunto=grande")
ok(st == 404, f"conjunto ampliado no disponible -> {st} (se espera 404)")
st, d, _ = get("/analitica/q1?conjunto=xx")
ok(st == 422, f"conjunto invalido -> {st} (se espera 422)")

print("\nRESULTADO:", "API ANALITICA SUPERADA" if not fallos else f"{len(fallos)} FALLAS")
sys.exit(1 if fallos else 0)