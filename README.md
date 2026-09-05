# Tech Challenge Fase 3 — State of Data Brasil

Pipeline de Big Data & Analytics para diagnosticar o mercado brasileiro de
Dados, Analytics e IA, usando as 3 últimas edições da pesquisa **State of
Data Brasil** (Data Hackers + Bain). Projeto desenvolvido na AWS Academy Lab
(S3, Glue, Athena) para o Tech Challenge Fase 3 da POSTECH.

## Grupo

- Pedro Costa da Silva
- Herbert Santos de Sousa
- Gustavo Soares Batista
- Erick Stefanic Belusci
- Cristian Tomio Kobayashi Taguchi

## Estrutura do repositório

```
.
├── requirements.txt                           # Dependências Python para rodar os notebooks/scripts locais
├── docs/
│   ├── State_of_Data_Brasil_Executivo.pptx      # Material executivo original
│   ├── State_of_Data_Brasil_Executivo_v2.pptx   # Mesma estrutura, gráficos/números atualizados (Athena v2, pós-correção da Silver) -- o grupo decide qual entra na entrega
│   └── arquitetura_aws_state_of_data.drawio     # Diagrama da arquitetura AWS
├── pipeline/
│   ├── config.py                             # Mapeamentos/paths compartilhados pelos 3 jobs
│   ├── bronze_job.py                         # Job 1/3 — RAW -> BRONZE
│   ├── silver_job.py                         # Job 2/3 — BRONZE -> SILVER
│   ├── gold_job.py                           # Job 3/3 — SILVER -> GOLD
│   └── pipeline_bronze_silver_gold.py        # Execução anterior, mantida só como histórico
├── sql/
│   ├── criar_tabelas_gold_athena.sql         # DDL das 8 tabelas Gold no Athena
│   └── queries_athena.sql                    # Queries que respondem às 7 perguntas de negócio
├── graficos/
│   ├── gerar_graficos.ipynb                  # Gera os 9 gráficos executivos a partir do Athena (database v2)
│   └── output/
│       ├── *.png                             # Gráficos gerados (usados no material executivo)
│       └── dados/*.csv                       # Dados brutos por trás de cada gráfico (conferência dos números)
├── validacao_pipeline/
│   └── comparar_databases_athena.ipynb       # Notebook de apoio: compara os databases Athena v1 x v2 (não é entrega, só validação)
├── analise_inicial/
│   ├── eda_pandas_inicial.py                 # EDA em Pandas usado para validar a lógica antes do PySpark
│   ├── gold_data.json                        # Métricas agregadas da EDA inicial (não é a fonte oficial)
│   └── data/
│       └── silver_unificado_3edicoes.csv     # Base tratada e unificada (14.005 respondentes)
└── bases/
    ├── Final Dataset - State of Data 2024 - Kaggle - df_survey_2024.csv
    ├── Final Dataset - State of Data 2025-2026 - Kaggle.csv
    └── State_of_data_BR_2023_Kaggle - df_survey_2023.csv
```

## Arquitetura da solução

Pipeline em camadas Bronze → Silver → Gold, 100% em AWS (diagrama completo
em `docs/arquitetura_aws_state_of_data.drawio` — abra em
[app.diagrams.net](https://app.diagrams.net), *File > Open From > Device*):

1. **Ingestão**: os 3 CSVs sobem para `s3://<bucket>/raw/`
2. **Bronze**: Glue Job (`bronze_job.py`, PySpark) lê os CSVs e grava em Parquet, particionado por `ano_pesquisa`
3. **Silver**: Glue Job (`silver_job.py`) faz limpeza, padronização de categorias (gênero, uso de IA, cargo) e unificação das 3 edições em schema comum
4. **Gold**: Glue Job (`gold_job.py`) gera as agregações de negócio que respondem às 7 perguntas do desafio
5. **Catalog**: as 8 tabelas Gold são criadas no Glue Data Catalog (`database state_of_data_gold_v2`) via `sql/criar_tabelas_gold_athena.sql`
6. **Consumo**: consultas SQL no Amazon Athena (`sql/queries_athena.sql`) + notebook de gráficos (`graficos/gerar_graficos.ipynb`) + DataViz no material executivo

## Decisões de arquitetura do pipeline

O pipeline é dividido em **3 Glue Jobs separados por camada**
(`bronze_job.py` → `silver_job.py` → `gold_job.py`), com os mapeamentos e
paths compartilhados (`COLMAP`, `ARQUIVOS`, `REGIAO_POR_UF`,
`LINGUAGEM_PREFIXO`, `BUCKET`/`PATHS`) centralizados em `config.py`.

**Por que 3 Jobs em vez de um só:**

- **Reprocessamento.** Bronze e Silver são as etapas mais pesadas
  (leem os 3 CSVs inteiros / fazem o union e a limpeza). Se só uma regra da
  camada Gold mudar, dá pra rodar só `gold_job.py` de novo, sem reprocessar
  Bronze e Silver do zero.
- **Isolamento de falha.** Se `gold_job.py` falhar, Bronze e Silver já
  estão persistidos no S3 e intactos — não precisa refazer o pipeline
  inteiro pra corrigir só a Gold.
- **Depuração.** No console do Glue, cada Job tem seu próprio
  histórico de execuções, logs e tempo de run — uma falha na Gold não vem
  misturada com o log de Bronze/Silver que já rodaram bem.

## Passo a passo de execução

### 1. Bucket e dados brutos

Crie/confirme o bucket S3 com as pastas `raw/`, `bronze/`, `silver/`,
`gold/`, e suba os 3 CSVs do Kaggle em `raw/` com os nomes exatamente como
em `pipeline/config.py` → `ARQUIVOS`.

### 2. Subir o `config.py` para o S3

Envie **só o `config.py`** para um prefixo de scripts do seu bucket, ex.:
`s3://<bucket>/scripts/config.py` (os outros 3 arquivos — `bronze_job.py`,
`silver_job.py`, `gold_job.py` — não precisam ir para o S3).

Depois de subir, abra o objeto `config.py` no console do S3 e copie a
**Object URL** dele (o link que aparece na aba "Object overview" — começa
com `https://`). É essa URL `https://...` que vai no
campo "Python library path" do Job — usar `s3://bucket/chave`
nesse campo **não funcionou** para o grupo, mesmo sendo o formato mais
comum na documentação da AWS.

### 3. Criar os 3 Glue Jobs

Para cada um dos 3 Jobs (`bronze`, `silver`, `gold`), no Glue Studio:

1. **ETL jobs → Create job → Script editor** (engine: Spark).
2. No editor de script, cole o conteúdo do arquivo correspondente
   (`bronze_job.py`, `silver_job.py` ou `gold_job.py`) direto no editor.
3. IAM Role: `LabRole`.

### 4. Configurar o `config.py` como Python library path (em cada um dos 3 Jobs)

1. Com o Job aberto, clique na aba **"Job details"** (fica ao lado de
   "Visual"/"Script", no topo da tela do Job).
2. Role a página até a seção **"Advanced properties"** e clique para
   expandir, se ela estiver recolhida.
3. Encontre o campo **"Python library path"**.
4. Cole ali a **Object URL** (`https://...`) do `config.py` que você
   copiou no passo 2.
5. Clique em **"Save"** no canto superior direito do Job.
6. Repita esses 5 passos nos **3 Jobs** (bronze, silver, gold) — é o
   mesmo `config.py`/mesma URL nos três.

### 5. Rodar na ordem correta

`bronze_job` → aguardar concluir → `silver_job` → aguardar concluir →
`gold_job`. Cada um só lê o que o anterior gravou no S3, então não dá pra
rodar fora de ordem (ex.: Gold antes de Silver falha por não achar dado em
`silver/`).

### 6. Catalogar as tabelas Gold no Athena

Rode `sql/criar_tabelas_gold_athena.sql` (uma instrução `CREATE TABLE` por
vez — o Athena não aceita múltiplas na mesma execução, e lembre de trocar
o bucket em todas as linhas `LOCATION` antes de rodar). **Não use um Glue
Crawler apontando direto pra `gold/`** — as 8 subpastas têm nomes
parecidos (`q1_...`, `q2_...`) e o crawler tende a agrupá-las como
partições de uma única tabela em vez de 8 tabelas separadas.

### 7. Validar com as queries de negócio

Rode `sql/queries_athena.sql` para conferir as respostas às 7 perguntas do
desafio.

## Notebooks auxiliares (validação e gráficos)

Além do pipeline em si, o repositório tem 2 notebooks Python que consomem
as tabelas Gold direto do Athena (via `awswrangler`/`boto3`, com
credenciais temporárias do AWS Academy Lab coladas manualmente em uma
célula no topo de cada um — nunca commitadas). Para rodá-los localmente,
instale as dependências com:

```
pip install -r requirements.txt
```

- **`validacao_pipeline/comparar_databases_athena.ipynb`** — compara
  contagens e valores entre dois databases Athena (ex.: o pipeline antigo
  x o novo, após uma migração de bucket) para garantir que a refatoração
  do pipeline não mudou os resultados de negócio. É um notebook de apoio
  para a migração feita neste projeto — não faz parte da entrega final.
- **`graficos/gerar_graficos.ipynb`** — busca as 9 queries de
  `sql/queries_athena.sql` (sem duplicar a lógica SQL, a regra já está
  toda na query), monta os 9 gráficos executivos com a paleta e as
  especificações visuais usadas no material, salva os PNGs em
  `graficos/output/` e os dados brutos de cada um em
  `graficos/output/dados/*.csv` (para conferir os números exatos sem
  depender dos rótulos arredondados do gráfico). Foi esse notebook que
  gerou os números usados em `docs/State_of_Data_Brasil_Executivo_v2.pptx`.

## Checkpoint de conferência

Depois de rodar os 3 Jobs, confira no Athena que as 8 tabelas Gold têm
contagens de linha consistentes com o run anterior (bucket anterior). Os
nomes de tabela, colunas e paths no S3 foram mantidos idênticos entre as
execuções — se algo não bater, é sinal de que vale a pena investigar antes
de seguir para a próxima etapa do roadmap (script de gráficos a partir do
Athena). **Confira também o total de respondentes por ano** (soma deve
bater com 14.005) — foi assim que o bug do `dropDuplicates()` (ver
"Decisões de arquitetura" acima) foi encontrado.

## Trocar de bucket (nova conta/execução no AWS Academy Lab)

Edite só a linha `BUCKET = "..."` em `pipeline/config.py` — os 3 Jobs
herdam o valor novo automaticamente via `PATHS`. Lembre de também
atualizar o `LOCATION` em `sql/criar_tabelas_gold_athena.sql` antes de
recriar as tabelas no Athena (o próprio arquivo já tem essa instrução), e
de repetir os passos 2 e 4 acima (novo `config.py`, nova Object URL) já
que o link do S3 muda com o bucket.

## Principais insights (ver material executivo para detalhes)

Reconferidos em `graficos/gerar_graficos.ipynb` contra o Athena
(`state_of_data_gold_v2`), já com a correção do `dropDuplicates()`
aplicada:

- Analista de Dados e Cientista de Dados somam 44% da base com cargo
  informado
- Adoção de IA no trabalho saltou de 80% (2023) para 98% (2025)
- Participação feminina vem caindo a cada edição: 24,4% (2023) → 23,5%
  (2024) → 22,0% (2025)
- Sudeste concentra 62% dos respondentes (2023–2025 combinado)
- Python e SQL dominam como linguagens de trabalho

> Senioridade (Júnior/Pleno/Sênior), distribuição regional e
> senioridade×região vieram idênticas ao material antigo — a correção
> só mudou números de cargos, respondentes/ano e gênero (ver diff entre
> `docs/State_of_Data_Brasil_Executivo.pptx` e `..._v2.pptx`).
