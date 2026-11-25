"""Valida Q1-Q5 de Spark contra un calculo independiente con pandas.

Preparacion (una vez, desde la raiz del repo):
  python3 -m venv .venv && . .venv/bin/activate && pip install pandas pyarrow
Uso:
  python3 spark/jobs/validar_consultas.py [--lago datalake/eventos] [--resultados datalake/resultados]
"""
import argparse
import glob
from datetime import timedelta

import pandas as pd


def spark_csv(dir_q):
    return pd.read_csv(glob.glob(f"{dir_q}/part-*.csv")[0])


def comparar(nombre, esperado, spark, claves, tol):
    spark = spark.copy()
    m = esperado.merge(spark, on=claves, how="outer", suffixes=("_pd", "_sp"), indicator=True)
    solo = int((m["_merge"] != "both").sum())
    peor = 0.0
    for col, t in tol.items():
        d = (m[f"{col}_pd"] - m[f"{col}_sp"]).abs().max()
        peor = max(peor, d / t if t else d)
    ok = solo == 0 and peor <= 1
    print(f"{'OK  ' if ok else 'FALLA'} {nombre}: {len(esperado)} filas pandas, {len(spark)} Spark, "
          f"filas sin pareja={solo}, mayor diferencia relativa a la tolerancia={peor:.3f}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lago", default="datalake/eventos")
    ap.add_argument("--resultados", default="datalake/resultados")
    a = ap.parse_args()

    ev = pd.read_parquet(a.lago)
    ev["mes"] = ev["mes"].astype(str)
    print(f"{len(ev)} eventos leidos de {a.lago}")
    ref = ev["timestamp"].max().floor("s")
    rev = ev[ev["estado_nuevo"].isin(["aprobada", "rechazada"])]
    res = []

    d = ev[(ev.estado_nuevo == "aprobada") & (ev.timestamp > ref - timedelta(days=90))]
    q1 = (d.assign(dia=d.timestamp.dt.strftime("%Y-%m-%d")).groupby(["dia", "modalidad"])
            .size().reset_index(name="aprobadas"))
    res.append(comparar("Q1", q1, spark_csv(f"{a.resultados}/q1"), ["dia", "modalidad"], {"aprobadas": 0.5}))

    q2 = (rev.groupby(["equipo_id", "mes"]).agg(aprobadas=("estado_nuevo", lambda s: (s == "aprobada").sum()),
                                                 revisadas=("estado_nuevo", "size")).reset_index())
    q2["proporcion"] = (q2.aprobadas / q2.revisadas).round(4)
    res.append(comparar("Q2", q2, spark_csv(f"{a.resultados}/q2"), ["equipo_id", "mes"],
                        {"aprobadas": 0.5, "revisadas": 0.5, "proporcion": 1e-4}))

    an = ev[(ev.estado_nuevo == "anotada") & ev.duracion_seg.notna()]
    q3 = (an.groupby(["modalidad", "tipo_tarea"]).duracion_seg
            .agg(n="size", media_seg="mean", p95_seg=lambda s: s.quantile(0.95)).reset_index().round(2))
    res.append(comparar("Q3", q3, spark_csv(f"{a.resultados}/q3"), ["modalidad", "tipo_tarea"],
                        {"n": 0.5, "media_seg": 0.011, "p95_seg": 0.011}))

    q4 = (rev.groupby(["cliente_id", "proyecto_id"]).agg(
            rechazadas=("estado_nuevo", lambda s: (s == "rechazada").sum()),
            revisadas=("estado_nuevo", "size")).reset_index())
    q4["proporcion"] = (q4.rechazadas / q4.revisadas).round(4)
    res.append(comparar("Q4", q4, spark_csv(f"{a.resultados}/q4"), ["cliente_id", "proyecto_id"],
                        {"rechazadas": 0.5, "revisadas": 0.5, "proporcion": 1e-4}))

    ap_ = ev[(ev.estado_nuevo == "aprobada") & ev.toma_ts.notna()].copy()
    ap_["espera"] = (ap_.timestamp - ap_.toma_ts).dt.total_seconds()
    ap_["inc"] = (ap_.espera > ap_.sla_seg).astype(int)
    q5 = (ap_.groupby("cliente_id").agg(n=("espera", "size"), p50_seg=("espera", lambda s: s.quantile(0.5)),
                                        p95_seg=("espera", lambda s: s.quantile(0.95)),
                                        pct_incumple_sla=("inc", lambda s: s.mean() * 100)).reset_index().round(2))
    res.append(comparar("Q5", q5, spark_csv(f"{a.resultados}/q5"), ["cliente_id"],
                        {"n": 0.5, "p50_seg": 0.02, "p95_seg": 0.02, "pct_incumple_sla": 0.02}))

    print("\nRESULTADO GLOBAL:", "TODAS COINCIDEN" if all(res) else "HAY DIFERENCIAS")


if __name__ == "__main__":
    main()