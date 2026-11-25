"""Compactacion del datalake: une los archivos pequenos que dejan los micro-lotes.

Cada micro-lote del Historial publica un archivo `evt-*.parquet`. Con el tiempo
una particion `mes=AAAA-MM/` acumula muchos archivos pequenos, lo que encarece la
planificacion y la lectura de Spark. Este script reemplaza, por particion, todos
los `evt-*` y `compact-*` por UN archivo `compact-*.parquet`:

  1. Lee los archivos de la particion y elimina filas repetidas por evento_id.
  2. Escribe el resultado en un archivo oculto (.tmp-compact-*), lo sincroniza
     a disco y lo publica con os.replace (Spark nunca lo lee a medias).
  3. Recien entonces borra los archivos de origen.

No toca los archivos `part-*` que genera Spark (generador del historial base).

Entre el paso 2 y el 3 (milisegundos) un lector podria ver los eventos en ambos
archivos; por eso conviene ejecutarlo sin consultas en curso o deduplicar por
evento_id en la consulta. Un archivo `.tmp-compact-*` huerfano (caida a mitad de
la escritura) es ignorado por Spark y se limpia en la siguiente ejecucion.

Uso (dentro del contenedor del historial):
  python /app/compactar.py [--lago /datalake/eventos] [--mes AAAA-MM] [--minimo 2] [--dry-run]
"""
import argparse
import glob
import json
import os
import time
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq


def compactar_particion(carpeta, minimo, dry_run):
    for huerfano in glob.glob(f"{carpeta}/.tmp-compact-*"):
        if not dry_run:
            os.remove(huerfano)
    origen = sorted(glob.glob(f"{carpeta}/evt-*.parquet") + glob.glob(f"{carpeta}/compact-*.parquet"))
    res = {"particion": os.path.basename(carpeta), "archivos_antes": len(origen),
           "archivos_despues": len(origen), "filas_antes": 0, "filas_despues": 0,
           "duplicados_eliminados": 0, "compactada": False}
    if len(origen) < minimo:
        return res
    tablas = [pq.read_table(f) for f in origen]
    tabla = pa.concat_tables(tablas)
    res["filas_antes"] = tabla.num_rows
    vistos, conservar = set(), []
    for i, ev in enumerate(tabla["evento_id"].to_pylist()):
        if ev not in vistos:
            vistos.add(ev)
            conservar.append(i)
    tabla = tabla.take(conservar)
    res["filas_despues"] = tabla.num_rows
    res["duplicados_eliminados"] = res["filas_antes"] - tabla.num_rows
    if dry_run:
        return res
    marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    final = f"{carpeta}/compact-{marca}-{uuid.uuid4().hex[:8]}.parquet"
    tmp = f"{carpeta}/.tmp-compact-{uuid.uuid4().hex}.parquet"
    try:
        pq.write_table(tabla, tmp, compression="snappy")
        with open(tmp, "rb") as f:
            os.fsync(f.fileno())
        os.chmod(tmp, 0o666)
        os.replace(tmp, final)                      # publicacion atomica
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    for f in origen:                                # solo despues de publicar el compactado
        os.remove(f)
    res["archivos_despues"] = 1
    res["compactada"] = True
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lago", default=os.environ.get("LAGO_DIR", "/datalake/eventos"))
    ap.add_argument("--mes", help="solo esta particion, p. ej. 2026-11")
    ap.add_argument("--minimo", type=int, default=2, help="archivos minimos para compactar")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    t0 = time.perf_counter()
    patron = f"mes={a.mes}" if a.mes else "mes=*"
    resultados = [compactar_particion(c, a.minimo, a.dry_run)
                  for c in sorted(glob.glob(f"{a.lago}/{patron}")) if os.path.isdir(c)]
    for r in resultados:
        print(f"{r['particion']}: {r['archivos_antes']} -> {r['archivos_despues']} archivos, "
              f"{r['filas_antes']} -> {r['filas_despues']} filas "
              f"({r['duplicados_eliminados']} duplicados eliminados)")
    print("RESUMEN_COMPACTACION_JSON", json.dumps({
        "dry_run": a.dry_run, "segundos": round(time.perf_counter() - t0, 3),
        "particiones": resultados}))


if __name__ == "__main__":
    main()