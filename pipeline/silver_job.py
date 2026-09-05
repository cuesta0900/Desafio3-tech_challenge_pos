"""
Tech Challenge Fase 3 - Pipeline State of Data Brasil (AWS Glue / PySpark)
============================================================================
CAMADA SILVER -- Job 2 de 3 (bronze_job.py -> silver_job.py -> gold_job.py)

BRONZE (S3) -> SILVER (S3): le as 3 particoes Bronze (uma por ano), aplica
o mapeamento de colunas (COLMAP), padroniza categorias (genero, uso de IA,
regiao a partir da UF) e unifica as 3 edicoes num schema comum, gravado em
Parquet particionado por ano_pesquisa.

Depende de config.py (deploy: configure "Python library path", em "Job details" do Glue Job)
e da camada Bronze ja gerada por bronze_job.py.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config import PATHS, ARQUIVOS, COLMAP, REGIAO_POR_UF

spark = SparkSession.builder.appName("state_of_data_brasil_silver").getOrCreate()


def extrai_edicao(ano: int) -> DataFrame:
    """Le a particao Bronze do ano e extrai so as colunas-chave, ja renomeadas."""
    df = spark.read.parquet(f"{PATHS['bronze']}/ano_pesquisa={ano}")
    m = COLMAP[ano]
    cols = [F.col(f"`{m[campo]}`").alias(campo) for campo in
            ["genero", "uf", "regiao", "cargo", "senioridade", "faixa_salarial", "usa_ia"]
            if m[campo] in df.columns]
    return df.select(F.lit(ano).alias("ano_pesquisa"), *cols)


def classifica_genero(col):
    c = F.lower(F.trim(col))
    return (
        F.when(c.contains("masc"), "Masculino")
         .when(c.contains("femin"), "Feminino")
         .when(c.contains("prefiro") | c.contains("não"), "Prefiro não informar")
         .when(col.isNull(), "Não informado")
         .otherwise("Outros")
    )


def classifica_uso_ia(col):
    """Resposta e multi-select (categorias separadas por virgula) -> escolhe
    a categoria de maior investimento/prioridade presente na resposta."""
    c = F.lower(col)
    return (
        F.when(col.isNull(), "Não respondeu")
         .when(c.contains("não utilizo nenhum tipo"), "Não usa IA")
         .when(c.contains("empresa em que trabalho paga"), "Usa - empresa paga")
         .when(c.contains("pago do meu próprio bolso"), "Usa - paga do bolso")
         .when(c.contains("ai para código") | c.contains("copilot"), "Usa - copilot/código")
         .when(c.contains("apenas soluções gratuitas"), "Usa - versão gratuita")
         .otherwise("Usa - outro")
    )


def gera_silver():
    dfs = [extrai_edicao(ano) for ano in ARQUIVOS]
    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d, allowMissingColumns=True)

    df = df.withColumn("genero_padrao", classifica_genero(F.col("genero")))
    df = df.withColumn("usa_ia_padrao", classifica_uso_ia(F.col("usa_ia")))
    df = df.withColumn("cargo", F.trim(F.col("cargo")))
    # 2024 usa uma grafia ligeiramente diferente de 2023/2025 para o mesmo cargo
    df = df.withColumn(
        "cargo",
        F.when(
            F.col("cargo") == "Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect",
            F.lit("Engenheiro de Dados/Data Engineer/Data Architect"),
        ).otherwise(F.col("cargo")),
    )
    df = df.withColumn("senioridade", F.trim(F.col("senioridade")))

    # regiao a partir da UF quando o campo regiao vier vazio
    mapping_expr = F.create_map([F.lit(x) for pair in REGIAO_POR_UF.items() for x in pair])
    df = df.withColumn(
        "regiao",
        F.coalesce(F.col("regiao"), mapping_expr.getItem(F.upper(F.col("uf"))), F.lit("Não informado")),
    )

    df.write.mode("overwrite").partitionBy("ano_pesquisa").parquet(PATHS["silver"])
    print(f"[SILVER] {df.count()} linhas gravadas em {PATHS['silver']}")


if __name__ == "__main__":
    gera_silver()
    spark.stop()
