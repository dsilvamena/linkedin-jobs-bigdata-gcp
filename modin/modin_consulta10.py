"""Promedio de skills por oferta: reducir el texto a conteos antes del join."""

import time
import pandas as pandas
import sqlite3
import tempfile
import pyarrow.fs as fs
import pyarrow.parquet as pq
from modin.pandas.io import to_pandas

from modin_comun import (
    pd, POSTINGS_PARQUET, iterar_habilidades_unicas, iniciar_ray, cerrar_ray, guardar,
)


def contar_habilidades(lotes):
    partes = []
    for lote in lotes:
        habilidades = pd.DataFrame(lote)
        texto = habilidades["job_skills"].fillna("")
        # Equivale a len(split(', ')), sin crear listas ni expandir filas.
        cantidad = (texto.str.count(", ") + 1) * (texto.str.strip() != "").astype("int64")
        partes.append(to_pandas(habilidades[["job_link"]].assign(cantidad_habilidades=cantidad)))
    if not partes:
        return pandas.DataFrame({"job_link": pandas.Series(dtype="object"),
                                 "cantidad_habilidades": pandas.Series(dtype="int64")})
    return pandas.concat(partes, ignore_index=True)


def calcular(ofertas, conteos):
    ofertas = ofertas[["job_link"]].drop_duplicates("job_link", keep="first")
    cruzadas = ofertas.merge(conteos, on="job_link", how="left")
    cantidad = cruzadas["cantidad_habilidades"].fillna(0)
    filas = len(cruzadas)
    total = int(cantidad.sum())
    return pandas.DataFrame([{
        "ofertas": filas,
        "ofertas_con_habilidades": int((cantidad > 0).sum()),
        "promedio_habilidades_por_oferta": round(total / filas, 2) if filas else None,
    }])


def calcular_en_disco(ruta_postings, lotes_habilidades, batch_size=10000):
    """Deduplica y cruza en SQLite para no cargar millones de enlaces en RAM."""
    sistema, ruta = fs.FileSystem.from_uri(str(ruta_postings))
    info = sistema.get_file_info(ruta)
    if info.is_file:
        archivos = [ruta]
    else:
        archivos = sorted(
            item.path
            for item in sistema.get_file_info(fs.FileSelector(ruta, recursive=True))
            if item.is_file and item.path.endswith(".parquet")
        )
    if not archivos:
        raise FileNotFoundError(f"No hay postings Parquet en {ruta_postings}")

    with tempfile.TemporaryDirectory(prefix="modin_c10_") as temporal:
        conexion = sqlite3.connect(f"{temporal}/consulta10.sqlite")
        conexion.execute("PRAGMA journal_mode=OFF")
        conexion.execute("PRAGMA synchronous=OFF")
        conexion.execute("PRAGMA temp_store=FILE")
        conexion.execute("CREATE TABLE skills (job_link TEXT PRIMARY KEY, cantidad INTEGER) WITHOUT ROWID")
        conexion.execute("CREATE TABLE ofertas (job_link TEXT PRIMARY KEY) WITHOUT ROWID")
        try:
            for lote in lotes_habilidades:
                filas = []
                for link, texto in zip(lote["job_link"], lote["job_skills"]):
                    clave = "\0" if link is None else "1" + str(link)
                    texto = "" if texto is None else str(texto)
                    cantidad = texto.count(", ") + 1 if texto.strip() else 0
                    filas.append((clave, cantidad))
                conexion.executemany(
                    "INSERT OR IGNORE INTO skills(job_link, cantidad) VALUES (?, ?)", filas
                )
                conexion.commit()

            # Las ofertas sin skills se insertan con cero; duplicados se ignoran.
            for ruta_archivo in archivos:
                with sistema.open_input_file(ruta_archivo) as archivo:
                    parquet = pq.ParquetFile(archivo, pre_buffer=False)
                    for lote in parquet.iter_batches(
                        batch_size=batch_size, columns=["job_link"], use_threads=False
                    ):
                        links = lote.column("job_link").to_pylist()
                        conexion.executemany(
                            "INSERT OR IGNORE INTO ofertas(job_link) VALUES (?)",
                            [("\0" if link is None else "1" + str(link),) for link in links],
                        )
                    conexion.commit()

            ofertas, con_habilidades, total = conexion.execute(
                """SELECT COUNT(*),
                          SUM(COALESCE(skills.cantidad, 0) > 0),
                          SUM(COALESCE(skills.cantidad, 0))
                   FROM ofertas LEFT JOIN skills USING(job_link)"""
            ).fetchone()
            return pandas.DataFrame([{
                "ofertas": ofertas,
                "ofertas_con_habilidades": int(con_habilidades or 0),
                "promedio_habilidades_por_oferta": (
                    round(total / ofertas, 2) if ofertas else None
                ),
            }])
        finally:
            conexion.close()


def main():
    print("=== MODIN + ARROW - CONSULTA 10 ===", flush=True)
    iniciar_ray()
    try:
        inicio = time.perf_counter()
        resultado = calcular_en_disco(POSTINGS_PARQUET, iterar_habilidades_unicas())
        guardar(10, resultado, time.perf_counter() - inicio)
    finally:
        cerrar_ray()


if __name__ == "__main__":
    main()
