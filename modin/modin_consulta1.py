import time

from modin_comun import (
    pd,
    POSTINGS_PARQUET,
    iniciar_ray,
    guardar_resultado,
    guardar_tiempo,
    cerrar_ray,
)

iniciar_ray()

print("=== MODIN - CONSULTA 1 ===")
print("Eliminar duplicados por job_link")

inicio = time.perf_counter()

df = pd.read_parquet(
    POSTINGS_PARQUET,
    columns=["job_link"]
)

filas_antes = len(df)

duplicados = int(
    df["job_link"]
    .duplicated()
    .sum()
)

filas_despues = (
    filas_antes - duplicados
)

segundos = (
    time.perf_counter() - inicio
)

resultado = pd.DataFrame([{
    "filas_originales": filas_antes,
    "filas_sin_duplicados": filas_despues,
    "duplicados_eliminados": duplicados,
}])

guardar_resultado(
    1,
    resultado
)

guardar_tiempo(
    1,
    segundos
)

print("\nRESULTADOS")
print(resultado)

print(
    f"Tiempo de ejecución: "
    f"{segundos:.4f} segundos"
)

cerrar_ray()