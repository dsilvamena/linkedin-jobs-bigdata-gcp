"""Consulta 7: lectura por lotes con Arrow; cruce y promedio con Modin."""

import time
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as fs
import pyarrow.parquet as pq


def leer_longitudes(ruta, batch_size=2000):
    if batch_size <= 0:
        raise ValueError("batch_size debe ser positivo")
    sistema, carpeta = fs.FileSystem.from_uri(str(ruta))
    if sistema.get_file_info(f"{carpeta}/_SUCCESS").type != fs.FileType.File:
        raise FileNotFoundError(f"Falta {ruta}/_SUCCESS: comprueba el preprocesamiento")
    archivos = sorted(
        info.path for info in sistema.get_file_info(fs.FileSelector(carpeta))
        if info.is_file and info.base_name.startswith("parte-")
        and info.path.endswith(".parquet")
    )
    if not archivos:
        raise FileNotFoundError(f"No hay partes Parquet en {ruta}")
    partes = []
    esquema = pa.schema([("job_link", pa.string()), ("longitud_caracteres", pa.int64())])
    for numero, ruta_archivo in enumerate(archivos, 1):
        with sistema.open_input_file(ruta_archivo) as archivo:
            parquet = pq.ParquetFile(archivo, pre_buffer=False)
            for lote in parquet.iter_batches(
                batch_size=batch_size, columns=["job_link", "job_summary"], use_threads=False,
            ):
                links = lote.column("job_link")
                longitudes = pc.utf8_length(lote.column("job_summary"))
                validas = pc.and_(pc.not_equal(links, ""), pc.greater(longitudes, 0))
                partes.append(pa.Table.from_arrays([
                    pc.cast(pc.filter(links, validas), pa.string()),
                    pc.cast(pc.filter(longitudes, validas), pa.int64()),
                ], schema=esquema))
        print(f"Parte {numero}/{len(archivos)} leida", flush=True)
    return pa.concat_tables(partes) if partes else pa.Table.from_batches([], schema=esquema)


def calcular(niveles, longitudes):
    # Deduplicar antes de filtrar, igual que las consultas Dask y Polars.
    niveles = niveles.drop_duplicates(subset=["job_link"], keep="first")
    niveles = niveles.dropna(subset=["job_link", "job_level"])
    niveles = niveles[niveles["job_level"].str.strip() != ""]
    cruzadas = longitudes.merge(niveles, on="job_link", how="inner")
    resultado = (
        cruzadas.groupby("job_level")["longitud_caracteres"]
        .agg(["count", "mean"])
        .rename(columns={"count": "descripciones", "mean": "promedio_caracteres"})
        .reset_index()
        .sort_values("job_level")
        .reset_index(drop=True)
    )
    resultado["promedio_caracteres"] = resultado["promedio_caracteres"].round(2)
    return resultado


def main():
    from modin_comun import (
        pd, POSTINGS_PARQUET, SUMMARY_PARQUET, iniciar_ray, cerrar_ray, guardar,
    )
    from modin.pandas.io import to_pandas

    print("=== MODIN + ARROW - CONSULTA 7 ===", flush=True)
    iniciar_ray()
    try:
        inicio = time.perf_counter()
        longitudes = pd.DataFrame(leer_longitudes(SUMMARY_PARQUET).to_pandas())
        niveles = pd.read_parquet(POSTINGS_PARQUET, columns=["job_link", "job_level"])
        resultado = to_pandas(calcular(niveles, longitudes))
        # Materializar antes de detener el reloj; la escritura queda fuera.
        guardar(7, resultado, time.perf_counter() - inicio)
    finally:
        cerrar_ray()


if __name__ == "__main__":
    main()
