import polars as pl
import time

from polars_comun import DATOS, RESULTADOS

# RUTA = "gs://utec-linkedin-jobs-2026/raw/linkedin_job_postings.csv"
RUTA = DATOS / "linkedin_job_postings.csv"

print("=== POLARS - CONSULTA 1 ===")
print("Eliminar duplicados por job_link")

inicio = time.time()

# Lectura del dataset desde Google Cloud Storage
df = pl.read_csv(RUTA)

filas_antes = df.height

# Eliminación de duplicados tomando job_link como identificador
df_limpio = df.unique(
    subset=["job_link"],
    keep="first"
)

filas_despues = df_limpio.height
duplicados_eliminados = filas_antes - filas_despues

fin = time.time()

RESULTADOS.mkdir(exist_ok=True)
(RESULTADOS / "consulta1_tiempo.txt").write_text(
    f"{fin - inicio:.6f}\n", encoding="utf-8"
)

print("\nRESULTADOS")
print("Filas originales:", filas_antes)
print("Filas después de eliminar duplicados:", filas_despues)
print("Duplicados eliminados:", duplicados_eliminados)
print(f"Tiempo de ejecución: {fin - inicio:.4f} segundos")

print("\nPrimeras 5 filas:")
print(
    df_limpio.select(
        ["job_link", "job_title", "company"]
    ).head(5)
)
