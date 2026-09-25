"""Pruebas de consulta 7 con Modin/Ray real y Parquet local, sin GCP."""

import os
os.environ.setdefault("MODIN_NPARTITIONS", "2")

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pandas
import pyarrow as pa
import pyarrow.parquet as pq
import ray

import modin_comun as comun
from modin.pandas.io import to_pandas
from modin_consulta7 import calcular, leer_longitudes, main


class Consulta7Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Modin conserva referencias de Ray: compartir sesion durante la suite.
        if not ray.is_initialized():
            import atexit
            ray.init(num_cpus=2, include_dashboard=False, logging_level="ERROR")
            atexit.register(ray.shutdown)

    def test_reglas_con_modin_ray(self):
        niveles = comun.pd.DataFrame({
            "job_link": ["a", "a", "b", "c", "d", "e", "e"],
            "job_level": ["Senior", "Otro", "Senior", " Senior ", " ", None, "Junior"],
        })
        longitudes = comun.pd.DataFrame({
            "job_link": ["a", "a", "b", "c", "d", "e", "sin_oferta"],
            "longitud_caracteres": [2, 4, 9, 7, 5, 5, 3],
        })
        self.assertEqual(to_pandas(calcular(niveles, longitudes)).to_dict("records"), [
            {"job_level": " Senior ", "descripciones": 1, "promedio_caracteres": 7.0},
            {"job_level": "Senior", "descripciones": 3, "promedio_caracteres": 5.0},
        ])

    def test_sin_coincidencias(self):
        niveles = comun.pd.DataFrame({"job_link": ["a"], "job_level": ["Senior"]})
        longitudes = comun.pd.DataFrame({"job_link": ["b"], "longitud_caracteres": [2]})
        self.assertTrue(to_pandas(calcular(niveles, longitudes)).empty)

    def test_dataset_incompleto_o_sin_partes(self):
        with tempfile.TemporaryDirectory() as carpeta:
            ruta = Path(carpeta)
            with self.assertRaises(FileNotFoundError):
                leer_longitudes(ruta.as_uri())
            (ruta / "_SUCCESS").touch()
            with self.assertRaises(FileNotFoundError):
                leer_longitudes(ruta.as_uri())

    def test_main_parquet_a_csv(self):
        with tempfile.TemporaryDirectory() as carpeta:
            ruta = Path(carpeta)
            resumen = ruta / "summary"
            resumen.mkdir()
            (resumen / "_SUCCESS").touch()
            for numero, textos in enumerate((["á😀", "abcd"], ["123456789", " "])):
                pq.write_table(pa.table({"job_link": ["a", "b"], "job_summary": textos}),
                               resumen / f"parte-{numero}.parquet")
            postings = ruta / "postings.parquet"
            pq.write_table(pa.table({"job_link": ["a", "b"], "job_level": ["Senior", "Senior"]}), postings)
            with patch.object(comun, "SUMMARY_PARQUET", resumen.as_uri()), \
                 patch.object(comun, "POSTINGS_PARQUET", postings.as_posix()), \
                 patch.object(comun, "RESULTADOS", ruta / "resultados"), \
                 patch.object(comun, "cerrar_ray"):
                main()
            resultado = pandas.read_csv(ruta / "resultados/consulta7.csv")
            self.assertEqual(resultado.to_dict("records"), [
                {"job_level": "Senior", "descripciones": 4, "promedio_caracteres": 4.0},
            ])
            self.assertGreater(float((ruta / "resultados/consulta7_tiempo.txt").read_text()), 0)

    def test_parquet_vacio(self):
        with tempfile.TemporaryDirectory() as carpeta:
            ruta = Path(carpeta)
            (ruta / "_SUCCESS").touch()
            esquema = pa.schema([("job_link", pa.string()), ("job_summary", pa.string())])
            pq.write_table(pa.Table.from_batches([], schema=esquema), ruta / "parte-0.parquet")
            longitudes = comun.pd.DataFrame(leer_longitudes(ruta.as_uri()).to_pandas())
            niveles = comun.pd.DataFrame({"job_link": ["a"], "job_level": ["Senior"]})
            self.assertTrue(to_pandas(calcular(niveles, longitudes)).empty)


if __name__ == "__main__":
    unittest.main()
