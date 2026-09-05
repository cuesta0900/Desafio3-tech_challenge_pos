"""
Tech Challenge Fase 3 - Pipeline State of Data Brasil (AWS Glue / PySpark)
============================================================================
CAMADA BRONZE -- Job 1 de 3 (bronze_job.py -> silver_job.py -> gold_job.py)

RAW (S3) -> BRONZE (S3): le os 3 CSVs originais e grava em Parquet, 1
particao por ano_pesquisa, com o schema BRUTO de cada edicao preservado --
a padronizacao de nomes so acontece na camada Silver (silver_job.py).

Depende de config.py (deploy: configure "Python library path", em "Job details" do Glue Job) 
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import PATHS, ARQUIVOS

spark = SparkSession.builder.appName("state_of_data_brasil_bronze").getOrCreate()


def gera_bronze():
    for ano, arquivo in ARQUIVOS.items():
        caminho = f"{PATHS['raw']}/{arquivo}"
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(caminho)
        )
        df = df.withColumn("ano_pesquisa", F.lit(ano))
        destino = f"{PATHS['bronze']}/ano_pesquisa={ano}"
        df.write.mode("overwrite").parquet(destino)
        print(f"[BRONZE] {ano}: {df.count()} linhas / {len(df.columns)} colunas -> {destino}")


if __name__ == "__main__":
    gera_bronze()
    spark.stop()
