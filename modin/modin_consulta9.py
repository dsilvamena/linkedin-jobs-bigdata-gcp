"""Ranking completo de empresas: solo enlaces y empresas, sin texto largo."""

import time
from modin.pandas.io import to_pandas

from modin_comun import pd, POSTINGS_PARQUET, iniciar_ray, cerrar_ray, guardar


def calcular(ofertas):
    ofertas = ofertas.drop_duplicates("job_link", keep="first")
    empresas = ofertas["company"].dropna()
    empresas = empresas[empresas.str.strip() != ""]
    return (empresas.value_counts().rename_axis("company").reset_index(name="ofertas")
            .sort_values(["ofertas", "company"], ascending=[False, True])
            .reset_index(drop=True))


def main():
    print("=== MODIN - CONSULTA 9 ===", flush=True)
    iniciar_ray()
    try:
        inicio = time.perf_counter()
        ofertas = pd.read_parquet(POSTINGS_PARQUET, columns=["job_link", "company"])
        resultado = to_pandas(calcular(ofertas))
        guardar(9, resultado, time.perf_counter() - inicio)
    finally:
        cerrar_ray()


if __name__ == "__main__":
    main()
