"""Ejecuta las diez consultas Dask con los CSV locales."""

from inicio import DATOS, RESULTADOS, cliente, ejecutar_consulta, reunir_tiempos


def main() -> None:
    with cliente() as conexion:
        print("Workers locales:", len(conexion.scheduler_info()["workers"]))
        for numero in range(1, 11):
            ejecutar_consulta(numero, DATOS, RESULTADOS)
    reunir_tiempos(RESULTADOS)
    print("\nTiempos guardados en", RESULTADOS / "tiempos_dask.csv")


if __name__ == "__main__":
    main()
