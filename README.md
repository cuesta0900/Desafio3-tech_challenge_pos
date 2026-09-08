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
├── requirements.txt                           # Dependências Python para rodar o notebook de gráficos
├── docs/
│   ├── State_of_Data_Brasil_Executivo_v2.pptx   # Material executivo (entrega principal)
│   └── arquitetura_aws_state_of_data.drawio     # Diagrama da arquitetura AWS
├── pipeline/
│   ├── config.py                             # Mapeamentos/paths compartilhados pelos 3 jobs
│   ├── bronze_job.py                         # Job 1/3 — RAW -> BRONZE
│   ├── silver_job.py                         # Job 2/3 — BRONZE -> SILVER
│   ├── gold_job.py                           # Job 3/3 — SILVER -> GOLD
│   └── pipeline_bronze_silver_gold.py        # Execução anterior, mantida só como histórico
├── sql/
│   ├── criar_tabelas_gold_athena.sql         # Cria o banco + as 8 tabelas Gold no Athena
│   └── queries_athena.sql                    # Queries que respondem às 7 perguntas de negócio
├── graficos/
│   ├── gerar_graficos.ipynb                  # Gera os 9 gráficos executivos a partir do Athena
│   └── output/*.png                          # Gráficos gerados (usados no material executivo)
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
5. **Catálogo**: as 8 tabelas Gold são criadas no Glue Data Catalog (`database state_of_data_gold_v2`) via `sql/criar_tabelas_gold_athena.sql`
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

Pensado para rodar do zero, numa conta AWS Academy Lab recém-iniciada (sem
nada criado ainda). Região usada pelo grupo: **us-east-1**.

### 1. Bucket S3 e dados brutos

1. No console da AWS, use a busca no topo e digite **S3** → abra o
   serviço **S3**.
2. Clique em **Create bucket**, dê um nome (precisa ser único
   globalmente, ex.: `state-of-data-seunome-2026`) e confirme a região
   **us-east-1**. Deixe o resto com as opções padrão e clique em
   **Create bucket**.
3. Dentro do bucket criado, use **Create folder** para criar as 4 pastas:
   `raw/`, `bronze/`, `silver/`, `gold/`.
4. Entre em `raw/` e faça upload (**Upload**) dos 3 CSVs da pasta `bases/`
   deste repositório, **mantendo os nomes de arquivo exatamente como
   estão** (o dicionário `ARQUIVOS` em `pipeline/config.py` já está
   calibrado para esses nomes).

### 2. Subir o `config.py` para o S3

Envie **só o `config.py`** (dentro de `pipeline/`) para um prefixo de
scripts do seu bucket, ex.: crie a pasta `scripts/` no mesmo bucket e
faça upload de `config.py` para lá (os outros 3 arquivos —
`bronze_job.py`, `silver_job.py`, `gold_job.py` — **não precisam** ir
para o S3, eles são colados direto no editor no passo 3).

Depois de subir, clique no objeto `config.py` dentro do console do S3 e
copie a **Object URL** dele (o link que aparece na aba "Object overview"
— começa com `https://`, não com `s3://`). É essa URL que vai no campo
"Python library path" do Job (passo 4) — usar `s3://bucket/chave` nesse
campo **não funcionou** para o grupo, mesmo sendo o formato mais comum na
documentação da AWS.

### 3. Criar os 3 Glue Jobs

Na busca do console, digite **Glue** → abra **AWS Glue**. No menu à
esquerda, clique em **ETL jobs**.

Repita os passos abaixo **3 vezes** (um para bronze, um para silver, um
para gold):

1. Clique em **Create job**, escolha o template **Script editor** e
   engine **Spark**.
2. Abra o arquivo correspondente no seu editor local (`bronze_job.py`,
   `silver_job.py` ou `gold_job.py`, dentro de `pipeline/`), selecione
   todo o conteúdo (Ctrl+A / Cmd+A), copie e cole direto na área de
   script do Glue, substituindo o conteúdo de exemplo que já vem lá.
3. Dê um nome ao Job que deixe claro qual camada é (ex.:
   `state-of-data-bronze`, `state-of-data-silver`, `state-of-data-gold`).
4. Na seção **IAM Role**, selecione **LabRole** no dropdown (é o papel já
   liberado no AWS Academy Lab — não existe opção de criar um papel
   próprio nesse ambiente).
5. Clique em **Save** no canto superior direito.

### 4. Configurar o `config.py` como Python library path (nos 3 Jobs)

Repita nos **3 Jobs** criados no passo anterior:

1. Com o Job aberto, clique na aba **"Job details"** (fica ao lado de
   "Visual"/"Script", no topo da tela do Job).
2. Role a página até a seção **"Advanced properties"** e clique para
   expandir, se ela estiver recolhida.
3. Encontre o campo **"Python library path"**.
4. Cole ali a **Object URL** (`https://...`) do `config.py` que você
   copiou no passo 2 — **não** o caminho `s3://...`.
5. Clique em **"Save"** no canto superior direito do Job.

### 5. Rodar os Jobs na ordem correta

Na tela de cada Job, clique em **Run** (canto superior direito) e
acompanhe na aba **Runs** até o status virar **Succeeded** (leva de 2 a 5
minutos por Job). A ordem importa — cada um só lê o que o anterior
gravou no S3:

`bronze` → aguardar Succeeded → `silver` → aguardar Succeeded → `gold`

Se algum Job falhar (status **Failed**), abra a aba **Runs** e clique em
**Logs**/**Error logs** dessa execução — a causa mais comum é o nome do
bucket em `pipeline/config.py` (`BUCKET = "..."`) não bater com o bucket
real que você criou no passo 1.

### 6. Criar o banco de dados no Athena

**Esse passo é fácil de esquecer numa conta nova, e sem ele os próximos
passos falham** (a conta AWS Academy Lab não vem com nenhum banco/database
pré-criado no Athena).

1. Na busca do console, digite **Athena** → abra **Amazon Athena** →
   **Query editor**.
2. Se aparecer um aviso pedindo para configurar um **"query result
   location"**, clique em **Edit settings**, escolha (ou crie) uma pasta
   no seu bucket S3 para isso (ex.: `s3://<bucket>/athena-results/`) e
   salve — é só uma vez, o Athena precisa de um lugar no S3 pra guardar o
   resultado das consultas.
3. Abra o arquivo `sql/criar_tabelas_gold_athena.sql` deste repositório.
   A **primeira linha executável** é:
   ```sql
   CREATE DATABASE IF NOT EXISTS state_of_data_gold_v2;
   ```
   Cole **só essa linha** na caixa de query do Athena e clique em **Run**.
   Isso cria o banco `state_of_data_gold_v2`, que vai aparecer no painel
   à esquerda em "Database". Sem esse passo, todo `CREATE TABLE` do
   próximo passo falha com erro de "database does not exist" (ou
   similar) — é exatamente o que acontece rodando numa conta pela
   primeira vez.
4. No painel à esquerda, no dropdown de bancos ("Database"), selecione
   `state_of_data_gold_v2` para usá-lo como banco atual antes de seguir
   para o próximo passo.

### 7. Criar as 8 tabelas Gold no Athena

No mesmo `sql/criar_tabelas_gold_athena.sql`, agora rode **cada
`CREATE EXTERNAL TABLE` separadamente** (o Athena não aceita várias
instruções na mesma execução) — são 8 no total, uma por pergunta de
negócio. **Antes de rodar, troque `BUCKET` pelo nome real do seu bucket
em todas as linhas `LOCATION`.**

**Não use um Glue Crawler apontando direto pra `gold/`** — as 8
subpastas têm nomes parecidos (`q1_...`, `q2_...`) e o crawler tende a
agrupá-las como partições de uma única tabela em vez de 8 tabelas
separadas.

### 8. Validar com as queries de negócio

Rode `sql/queries_athena.sql` (também uma consulta por vez) para conferir
as respostas às 7 perguntas do desafio.

## Gráficos executivos

`graficos/gerar_graficos.ipynb` busca as 9 queries de
`sql/queries_athena.sql` (sem duplicar a lógica SQL — a regra já está
toda na query), monta os 9 gráficos executivos com a paleta e as
especificações visuais usadas no material, e salva os PNGs em
`graficos/output/`, prontos para colar em
`docs/State_of_Data_Brasil_Executivo_v2.pptx`.

Para rodar localmente: instale as dependências com
`pip install -r requirements.txt`, abra o notebook, cole suas
credenciais temporárias do AWS Academy Lab na célula da seção 1 (nunca
commitadas) e rode todas as células em ordem.

## Trocar de bucket (nova conta/execução no AWS Academy Lab)

Edite só a linha `BUCKET = "..."` em `pipeline/config.py` — os 3 Jobs
herdam o valor novo automaticamente via `PATHS`. Lembre de também
atualizar o `LOCATION` em `sql/criar_tabelas_gold_athena.sql` antes de
recriar as tabelas no Athena, e de repetir os passos 2 e 4 acima (novo
`config.py`, nova Object URL) já que o link do S3 muda com o bucket.

## Principais insights (ver material executivo para detalhes)

Com base nas queries de `sql/queries_athena.sql`, rodadas contra o
Athena (`database state_of_data_gold_v2`):

- Analista de Dados e Cientista de Dados somam 44% da base com cargo
  informado
- Adoção de IA no trabalho saltou de 80% (2023) para 98% (2025)
- Participação feminina vem caindo a cada edição: 24,4% (2023) → 23,5%
  (2024) → 22,0% (2025)
- Sudeste concentra 62% dos respondentes (2023–2025 combinado)
- Python e SQL dominam como linguagens de trabalho
