import os
from pathlib import Path

# ============================================================
# MODIN
# ============================================================

os.environ["MODIN_ENGINE"] = "ray"
os.environ["MODIN_NPARTITIONS"] = "16"

import modin.pandas as pd
import ray


RESULTADOS = Path(__file__).resolve().parent / "resultados"


# ============================================================
# RUTAS
# ============================================================

POSTINGS = (
    "gs://utec-linkedin-jobs-2026/raw/"
    "linkedin_job_postings.csv"
)

SKILLS = (
    "gs://utec-linkedin-jobs-2026/raw/"
    "job_skills.csv"
)

POSTINGS_PARQUET = (
    "gs://utec-linkedin-jobs-2026/"
    "processed_partitioned/postings_parquet"
)

SKILLS_PARQUET = (
    "gs://utec-linkedin-jobs-2026/"
    "processed_partitioned/skills_parquet"
)

SUMMARY_PARQUET = (
    "gs://utec-linkedin-jobs-2026/"
    "processed/job_summary_parquet"
)


# ============================================================
# RAY
# ============================================================

def iniciar_ray():
    if not ray.is_initialized():
        ray.init(
            address="auto",
            ignore_reinit_error=True,
            logging_level="ERROR",
        )


def cerrar_ray():
    if ray.is_initialized():
        ray.shutdown()


# ============================================================
# GUARDADO
# ============================================================

def guardar_tiempo(numero, segundos):
    RESULTADOS.mkdir(exist_ok=True)

    archivo = (
        RESULTADOS /
        f"consulta{numero}_tiempo.txt"
    )

    archivo.write_text(
        f"{segundos:.6f}\n",
        encoding="utf-8",
    )


def guardar_resultado(numero, resultado):
    RESULTADOS.mkdir(exist_ok=True)

    resultado.to_csv(
        RESULTADOS / f"consulta{numero}.csv",
        index=False,
    )


def guardar(numero, resultado, segundos):
    guardar_resultado(
        numero,
        resultado,
    )

    guardar_tiempo(
        numero,
        segundos,
    )

    print("\nRESULTADOS")
    print(resultado)

    print(
        f"\nTiempo de ejecución: "
        f"{segundos:.4f} segundos"
    )

 
def ofertas_unicas(columnas):
    columnas_lectura = list(
        dict.fromkeys(
            ["job_link", *columnas]
        )
    )

    df = pd.read_parquet(
        POSTINGS_PARQUET,
        columns=columnas_lectura,
    )

    return df.drop_duplicates(
        subset=["job_link"],
        keep="first",
    )


def iterar_habilidades_unicas(ruta=None, batch_size=2000):
    """Lee texto por lotes y deduplica enlaces en SQLite, sin un set gigante."""
    import sqlite3
    import tempfile
    import pyarrow.fs as fs
    import pyarrow.parquet as pq

    if batch_size <= 0:
        raise ValueError("batch_size debe ser positivo")
    sistema, carpeta = fs.FileSystem.from_uri(str(ruta or SKILLS_PARQUET))
    archivos = sorted(
        info.path for info in sistema.get_file_info(fs.FileSelector(carpeta, recursive=True))
        if info.is_file and info.path.endswith(".parquet")
    )
    if not archivos:
        raise FileNotFoundError(f"No hay archivos Parquet en {carpeta}")
    # El archivo temporal se elimina al cerrar el generador, incluso si hay error.
    with tempfile.TemporaryDirectory(prefix="modin_skills_") as temporal:
        conexion = sqlite3.connect(f"{temporal}/vistos.sqlite")
        conexion.execute("PRAGMA journal_mode=OFF")
        conexion.execute("PRAGMA synchronous=OFF")
        conexion.execute("PRAGMA temp_store=FILE")
        conexion.execute("CREATE TABLE vistos (job_link TEXT PRIMARY KEY) WITHOUT ROWID")
        try:
            for numero, ruta_archivo in enumerate(archivos, 1):
                with sistema.open_input_file(ruta_archivo) as archivo:
                    parquet = pq.ParquetFile(archivo, pre_buffer=False)
                    for lote in parquet.iter_batches(
                        batch_size=batch_size,
                        columns=["job_link", "job_skills"],
                        use_threads=False,
                    ):
                        tabla = lote.to_pandas().drop_duplicates("job_link", keep="first")
                        nuevas = []
                        for posicion, link in enumerate(tabla["job_link"]):
                            # SQLite permite varios NULL en UNIQUE; usamos una clave reservada.
                            clave = "\0" if link is None else "1" + str(link)
                            cursor = conexion.execute(
                                "INSERT OR IGNORE INTO vistos(job_link) VALUES (?)", (clave,)
                            )
                            if cursor.rowcount == 1:
                                nuevas.append(posicion)
                        conexion.commit()
                        if nuevas:
                            yield tabla.iloc[nuevas].reset_index(drop=True)
                print(f"Habilidades: parte {numero}/{len(archivos)} leida", flush=True)
        finally:
            conexion.close()
