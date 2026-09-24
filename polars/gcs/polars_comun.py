"""Rutas de Cloud Storage y operaciones compartidas por las consultas."""

from pathlib import Path
from functools import lru_cache
import importlib.util
import os
import shutil
import subprocess

import fsspec
import polars as pl


DATOS = "gs://utec-linkedin-jobs-2026/raw"
RESUMEN_PARQUET = os.getenv(
    "POLARS_JOB_SUMMARY_PARQUET",
    os.getenv(
        "DASK_JOB_SUMMARY_PARQUET",
        "gs://utec-linkedin-jobs-2026/processed/job_summary_parquet",
    ),
)
RESULTADOS = Path(__file__).resolve().parent / "resultados"


@lru_cache(maxsize=1)
def opciones_gcs() -> dict[str, str] | None:
    """Usa la sesión activa de gcloud o las credenciales de aplicación."""
    if shutil.which("gcloud"):
        proceso = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            capture_output=True,
            text=True,
            check=False,
        )
        token = proceso.stdout.strip()
        if proceso.returncode == 0 and token:
            return {"bearer_token": token}

    try:
        google_auth_disponible = importlib.util.find_spec("google.auth") is not None
    except ModuleNotFoundError:
        google_auth_disponible = False
    if google_auth_disponible:
        return None  # Polars utiliza su proveedor automático de credenciales.
    raise RuntimeError(
        "Para leer GCS, inicia sesión con gcloud o instala google-auth "
        "y configura credenciales de aplicación."
    )


def leer(nombre: str) -> pl.LazyFrame:
    # Los campos de estos tres CSV son texto; evita inferencias distintas entre archivos.
    return pl.scan_csv(
        f"{DATOS}/{nombre}", infer_schema=False, storage_options=opciones_gcs()
    )


def leer_resumen_parquet() -> pl.LazyFrame:
    """Lee las partes de job_summary preparadas para la consulta 7 de Dask."""
    ruta = RESUMEN_PARQUET.rstrip("/")
    if ruta.startswith("gs://"):
        opciones = opciones_gcs()
        token = opciones["bearer_token"] if opciones else None
        sistema, carpeta = fsspec.core.url_to_fs(ruta, token=token)
    else:
        sistema, carpeta = fsspec.core.url_to_fs(ruta)
    if not sistema.exists(f"{carpeta}/_SUCCESS"):
        raise FileNotFoundError(
            f"Falta el Parquet completo de job_summary en {ruta}. "
            "Ejecuta dask_proyecto/preparar_job_summary.py antes de la consulta 7."
        )
    return pl.scan_parquet(
        f"{ruta}/parte-*.parquet",
        storage_options=opciones_gcs() if ruta.startswith("gs://") else None,
        low_memory=True,
    )


def ofertas_unicas() -> pl.LazyFrame:
    """Una oferta por enlace para que las publicaciones duplicadas no sesguen métricas."""
    return leer("linkedin_job_postings.csv").unique(subset="job_link", keep="first")


def lista_habilidades() -> pl.Expr:
    """Convierte 'a, b' en ['a', 'b']; los campos vacíos producen una lista vacía."""
    texto = pl.col("job_skills")
    return (
        pl.when(texto.is_null() | (texto.str.strip_chars() == ""))
        .then(pl.lit([]).cast(pl.List(pl.String)))
        .otherwise(texto.str.split(", "))
        .alias("habilidades")
    )


def guardar(numero: int, resultado: pl.DataFrame, segundos: float) -> None:
    RESULTADOS.mkdir(exist_ok=True)
    resultado.write_csv(RESULTADOS / f"consulta{numero}.csv")
    (RESULTADOS / f"consulta{numero}_tiempo.txt").write_text(
        f"{segundos:.6f}\n", encoding="utf-8"
    )
    print("\nRESULTADOS")
    print(resultado)
    print(f"Tiempo de ejecución: {segundos:.4f} segundos")
