"""
Tech Challenge Fase 3 - Pipeline State of Data Brasil (AWS Glue / PySpark)
============================================================================
CAMADA GOLD -- Job 3 de 3 (bronze_job.py -> silver_job.py -> gold_job.py)

SILVER (S3) -> GOLD (S3): le a Silver unificada e gera as 8 tabelas
agregadas que respondem as 7 perguntas de negocio do desafio (uma pasta por
tabela, dentro de gold/).

Depende de config.py (deploy: configure "Python library path", em "Job details" do Glue Job)
e da camada Silver ja gerada por silver_job.py.

IMPORTANTE: os nomes das 8 tabelas Gold abaixo (q1_top_cargos, q2_..., etc.)
e o schema de cada uma devem permanecer EXATAMENTE como aqui -- as queries
em sql/queries_athena.sql e o futuro script de graficos (item 1 do roadmap)
dependem desses nomes e colunas.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import PATHS, ARQUIVOS, LINGUAGEM_PREFIXO

spark = SparkSession.builder.appName("state_of_data_brasil_gold").getOrCreate()


def extrai_linguagens(ano: int):
    """Long-format: uma linha por (respondente, linguagem utilizada)."""
    df = spark.read.parquet(f"{PATHS['bronze']}/ano_pesquisa={ano}")
    prefixo = LINGUAGEM_PREFIXO[ano]
    lang_cols = [c for c in df.columns if c.startswith(prefixo) and "não utilizo" not in c.lower()]
    if not lang_cols:
        return spark.createDataFrame([], "ano_pesquisa int, linguagem string")

    exprs = [F.when(F.col(f"`{c}`") == 1, F.lit(c)).otherwise(F.lit(None)) for c in lang_cols]
    arr = F.array(*exprs)
    out = (
        df.withColumn("linguagem_raw", F.explode(arr))
          .filter(F.col("linguagem_raw").isNotNull())
          .withColumn("ano_pesquisa", F.lit(ano))
          .select("ano_pesquisa", "linguagem_raw")
    )
    if ano == 2023:
        # coluna vem como "('P4_d_1 ', 'SQL')" -> extrai o texto entre as
        # ultimas aspas simples (a descricao da linguagem)
        out = out.withColumn("linguagem", F.regexp_extract(F.col("linguagem_raw"), r"'([^']+)'\)$", 1))
    else:
        # coluna vem como "4.d.14_JavaScript" -> extrai o texto apos o
        # ultimo "_" (o nome da linguagem)
        out = out.withColumn("linguagem", F.element_at(F.split(F.col("linguagem_raw"), "_"), -1))
    return out.select("ano_pesquisa", "linguagem")


def gera_gold():
    df = spark.read.parquet(PATHS["silver"])

    # Q1 - estrutura do mercado: cargos mais frequentes + respondentes/ano
    g1a = df.filter(F.col("cargo").isNotNull()).groupBy("cargo").count().orderBy(F.desc("count"))
    g1a.write.mode("overwrite").parquet(f"{PATHS['gold']}/q1_top_cargos")

    g1b = df.groupBy("ano_pesquisa").count()
    g1b.write.mode("overwrite").parquet(f"{PATHS['gold']}/q1_respondentes_por_ano")

    # Q2 - perfis mais valorizados: senioridade x faixa salarial
    g2 = df.groupBy("ano_pesquisa", "senioridade", "faixa_salarial").count()
    g2.write.mode("overwrite").parquet(f"{PATHS['gold']}/q2_senioridade_salario")

    # Q3 - diversidade de genero por ano
    g3 = df.groupBy("ano_pesquisa", "genero_padrao").count()
    g3.write.mode("overwrite").parquet(f"{PATHS['gold']}/q3_genero_por_ano")

    # Q5 - adocao de IA por ano
    g5 = df.filter(F.col("usa_ia_padrao") != "Não respondeu").groupBy("ano_pesquisa", "usa_ia_padrao").count()
    g5.write.mode("overwrite").parquet(f"{PATHS['gold']}/q5_ia_por_ano")

    # Q6 - diferencas regionais
    g6a = df.filter(F.col("regiao") != "Não informado").groupBy("ano_pesquisa", "regiao").count()
    g6a.write.mode("overwrite").parquet(f"{PATHS['gold']}/q6_regiao")

    g6b = df.groupBy("ano_pesquisa", "regiao", "senioridade").count()
    g6b.write.mode("overwrite").parquet(f"{PATHS['gold']}/q6_senioridade_regiao")

    # Q4 - tecnologias/linguagens mais adotadas (todas as edicoes)
    lang_dfs = [extrai_linguagens(ano) for ano in ARQUIVOS]
    g4 = lang_dfs[0]
    for d in lang_dfs[1:]:
        g4 = g4.unionByName(d)
    g4 = g4.groupBy("ano_pesquisa", "linguagem").count().orderBy(F.desc("count"))
    g4.write.mode("overwrite").parquet(f"{PATHS['gold']}/q4_linguagens_por_ano")

    print("[GOLD] Tabelas agregadas geradas com sucesso.")


if __name__ == "__main__":
    gera_gold()
    spark.stop()
