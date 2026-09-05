-- ============================================================================
-- Criação manual das tabelas GOLD no Athena (alternativa ao Crawler)
-- Rode esses comandos no Athena Query Editor, um de cada vez (ou todos juntos).
-- Troque BUCKET pelo nome real do seu bucket em TODAS as linhas LOCATION.
-- ============================================================================

-- CREATE DATABASE IF NOT EXISTS state_of_data_gold;
CREATE DATABASE IF NOT EXISTS state_of_data_gold_v2;

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold_v2.q1_top_cargos (
    cargo string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://BUCKET/gold/q1_top_cargos/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold_v2.q1_respondentes_por_ano (
    ano_pesquisa int,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://BUCKET/gold/q1_respondentes_por_ano/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold_v2.q2_senioridade_salario (
    ano_pesquisa int,
    senioridade string,
    faixa_salarial string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://BUCKET/gold/q2_senioridade_salario/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold_v2.q3_genero_por_ano (
    ano_pesquisa int,
    genero_padrao string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://BUCKET/gold/q3_genero_por_ano/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold_v2.q4_linguagens_por_ano (
    ano_pesquisa int,
    linguagem string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://BUCKET/gold/q4_linguagens_por_ano/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold_v2.q5_ia_por_ano (
    ano_pesquisa int,
    usa_ia_padrao string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://BUCKET/gold/q5_ia_por_ano/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold_v2.q6_regiao (
    ano_pesquisa int,
    regiao string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://BUCKET/gold/q6_regiao/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold_v2.q6_senioridade_regiao (
    ano_pesquisa int,
    regiao string,
    senioridade string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://BUCKET/gold/q6_senioridade_regiao/';
