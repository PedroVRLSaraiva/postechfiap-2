# Pipeline Híbrido de Alfabetização — Plano de Execução

> **Para quem for executar:** este plano assume que você não tem experiência prévia
> com GCP, Python de produção, ou engenharia de dados. Cada passo é pequeno (2-15 min)
> e vem com uma explicação do **o quê** e do **por quê**. Siga na ordem. Se um comando
> der erro, pare e leia a mensagem de erro antes de tentar de novo — geralmente ela diz
> exatamente o que falta.
>
> **REQUIRED SUB-SKILL:** Use `superpowers:executing-plans` para rodar este plano
> nesta mesma sessão, tarefa por tarefa, com checkpoints de revisão. (Não é indicado
> usar `subagent-driven-development` aqui porque você pediu para guiar a execução
> pessoalmente, passo a passo.)

**Objetivo:** construir a pipeline híbrida (batch + streaming) de dados sobre o
Indicador Criança Alfabetizada, com camadas Bronze/Silver/Gold no GCP, 100% dentro do
free tier, cumprindo os requisitos do Tech Challenge Fase 2.

**Arquitetura:** Cloud Scheduler aciona 3 Cloud Functions em cadeia (batch: ingest →
silver → gold); um script publica eventos simulados no Pub/Sub, que aciona uma 4ª
Cloud Function (streaming). Tudo lê/escreve em GCS (Parquet) e no BigQuery, com Great
Expectations validando a camada Silver e Cloud Monitoring alertando sobre falhas.

**Tech Stack:** Python 3.12, pandas, great_expectations==1.19.0, google-cloud-bigquery,
google-cloud-storage, google-cloud-pubsub, gcsfs, functions-framework, pytest.

## Global Constraints

- **Região GCP:** `us-central1` (barata, elegível a free tier) para todos os recursos.
- **Python:** 3.12 (runtime `python312` no Cloud Functions Gen2).
- **Versões fixadas** (testadas e confirmadas compatíveis entre si nesta sessão):
  `great_expectations==1.19.0`, `pandas`, `google-cloud-bigquery`,
  `google-cloud-storage`, `google-cloud-pubsub`, `gcsfs`, `functions-framework`,
  `db-dtypes`, `pytest`.
- **Placeholders que você substitui pelos seus valores reais** (aparecem em
  `MAIÚSCULO_COM_UNDERSCORE` no plano): `SEU_PROJECT_ID` (o ID do projeto GCP que você
  criar na Tarefa 1) e `SEU_EMAIL` (para receber alertas). Todo o resto do código é
  literal — não precisa adivinhar nada além desses dois valores.
- **Nomes de recursos GCP** (fixos, use exatamente estes):
  - Bucket GCS: `SEU_PROJECT_ID-pipeline-alfabetizacao`
  - Dataset BigQuery (Gold): `gold_alfabetizacao`
  - Tópico Pub/Sub: `alfabetizacao-eventos`
  - Cloud Functions: `ingest-batch`, `ingest-stream`, `process-silver`, `process-gold`
  - Cloud Scheduler jobs: `job-ingest-batch` (03:00), `job-process-silver` (03:10),
    `job-process-gold` (03:20) — horários escalonados para dar tempo de cada etapa
    terminar antes da próxima começar.
- **Cada Cloud Function é uma pasta autocontida** (`pipeline/<nome>/`) com sua própria
  cópia dos arquivos `gcs_utils.py`/`bq_utils.py`. Isso porque o Cloud Functions builda
  cada função isoladamente a partir da pasta que você aponta em `--source` — não há
  como duas functions compartilharem um pacote Python comum sem complicar o deploy.
  Escrevemos e testamos esse código uma vez em `pipeline/common/`, e cada tarefa de
  função copia os arquivos prontos para dentro da sua própria pasta antes do deploy.
- **Convenção de dados:** cada camada (bronze/silver/gold) grava em Parquet no GCS
  usando duas cópias — uma versionada por data (`dt=AAAA-MM-DD/`, preserva histórico
  completo) e uma sempre atualizada (`latest/`, para a próxima camada não precisar
  descobrir qual é a mais recente). A camada Gold, além do Parquet, carrega o
  resultado final também no BigQuery (dataset `gold_alfabetizacao`), para consulta
  SQL/dashboards.
- **Git:** uma branch por fase (nomes indicados em cada fase abaixo), commits
  pequenos e descritivos, uma Pull Request por branch antes do merge para `main`.

---

## Fase 0 — Setup do GCP e do repositório

**Branch:** `chore/setup-gcp-and-repo`

### Task 1: Criar o projeto GCP e ativar as APIs necessárias

**O que:** criar o projeto que vai hospedar toda a pipeline, e ligar (ativar) os
serviços do Google Cloud que vamos usar. No GCP, cada serviço (BigQuery, Cloud
Functions, etc.) precisa ser "ativado" explicitamente antes de usar — é uma proteção
para você não ser cobrado por serviços que nunca pediu para usar.

**Por quê:** sem isso, todo comando das próximas tarefas vai falhar com erro de "API
not enabled" ou "project not found".

**Passos:**

- [ ] **Passo 1: Acessar o console e criar o projeto**

Acesse https://console.cloud.google.com/projectcreate, dê um nome (ex.:
`pipeline-alfabetizacao`). O Google vai gerar um **Project ID** único (algo como
`pipeline-alfabetizacao-123456`) — anote esse ID exato, ele é o seu `SEU_PROJECT_ID`
para o resto do plano.

- [ ] **Passo 2: Confirmar que o projeto está com faturamento vinculado**

Vá em https://console.cloud.google.com/billing e confirme que o projeto novo está
associado a uma conta de faturamento (necessário mesmo para usar o free tier — o GCP
não cobra dentro dos limites gratuitos, mas exige um cartão cadastrado como
verificação).

- [ ] **Passo 3: Ativar as APIs necessárias**

No console, vá em "APIs e serviços" → "Biblioteca" e ative, uma por uma (busque pelo
nome exato):
  - BigQuery API
  - Cloud Storage API
  - Cloud Functions API
  - Cloud Build API (necessária internamente para o deploy das functions)
  - Cloud Run API (Cloud Functions Gen2 roda em cima do Cloud Run)
  - Pub/Sub API
  - Cloud Scheduler API
  - Cloud Logging API
  - Cloud Monitoring API

Isso pode ser feito também em um único comando depois que o CLI estiver instalado
(Tarefa 2) — mas fazer pela primeira vez pelo console ajuda a visualizar o que cada
serviço é.

- [ ] **Passo 4: Confirmar**

No console, em "APIs e serviços" → "Painel", confirme que todas aparecem como
"Ativada".

---

### Task 2: Instalar e autenticar o Google Cloud CLI localmente

**O que:** instalar o `gcloud` (ferramenta de linha de comando do GCP) no seu
computador e conectar ele à sua conta Google.

**Por quê:** todas as próximas tarefas usam comandos `gcloud`/`bq` no terminal em vez
de clicar no console — é mais rápido e reproduzível (também fica registrado no
logbook exatamente o que foi rodado).

**Passos:**

- [ ] **Passo 1: Instalar o Google Cloud CLI**

No macOS, com Homebrew:
```bash
brew install --cask google-cloud-sdk
```

- [ ] **Passo 2: Autenticar sua conta de usuário**

```bash
gcloud auth login
```
Isso abre o navegador para você logar com a conta Google que criou o projeto.

- [ ] **Passo 3: Configurar credenciais para bibliotecas Python (Application Default Credentials)**

```bash
gcloud auth application-default login
```
Isso é o que as bibliotecas Python (`google-cloud-bigquery`, etc.) usam para se
autenticar quando você rodar scripts localmente — é uma credencial separada do login
do `gcloud` em si.

- [ ] **Passo 4: Definir o projeto padrão**

```bash
gcloud config set project SEU_PROJECT_ID
```

- [ ] **Passo 5: Verificar que está tudo certo**

```bash
gcloud config list
```
Esperado: mostrar `account = seu-email` e `project = SEU_PROJECT_ID`.

---

### Task 3: Criar o bucket GCS, o dataset BigQuery e o tópico Pub/Sub

**O que:** criar os três "contêineres" onde a pipeline vai gravar dados: um bucket de
armazenamento de arquivos (GCS, para bronze/silver), um dataset no BigQuery (para
gold) e um tópico de mensagens (Pub/Sub, para streaming).

**Por quê:** são a infraestrutura de armazenamento da pipeline — precisam existir
antes de qualquer Cloud Function tentar gravar neles.

**Passos:**

- [ ] **Passo 1: Criar o bucket GCS**

```bash
gcloud storage buckets create gs://SEU_PROJECT_ID-pipeline-alfabetizacao \
  --location=us-central1 \
  --uniform-bucket-level-access
```
Expected: `Creating gs://SEU_PROJECT_ID-pipeline-alfabetizacao/...` seguido de sucesso.

- [ ] **Passo 2: Criar o dataset BigQuery (camada Gold)**

```bash
bq mk --dataset --location=us-central1 SEU_PROJECT_ID:gold_alfabetizacao
```
Expected: `Dataset 'SEU_PROJECT_ID:gold_alfabetizacao' successfully created.`

- [ ] **Passo 3: Criar o tópico Pub/Sub**

```bash
gcloud pubsub topics create alfabetizacao-eventos
```
Expected: `Created topic [projects/SEU_PROJECT_ID/topics/alfabetizacao-eventos].`

- [ ] **Passo 4: Verificar tudo**

```bash
gcloud storage ls
bq ls
gcloud pubsub topics list
```
Confirme que os três recursos aparecem.

---

### Task 4: Estruturar o repositório local e abrir a branch de setup

**O que:** criar as pastas do projeto, um ambiente virtual Python, e o
`requirements.txt` raiz usado para rodar testes localmente.

**Por quê:** organiza o código antes de começar a escrever qualquer lógica, e isola as
dependências Python do resto do seu sistema (boa prática, evita conflito de versões).

**Passos:**

- [ ] **Passo 1: Criar a estrutura de pastas**

```bash
cd "/Users/pedrosaraiva/FIAP - Tech Challenge - Fase 2"
mkdir -p pipeline/common pipeline/ingest_batch pipeline/ingest_stream \
  pipeline/process_silver pipeline/process_gold scripts tests
```

- [ ] **Passo 2: Criar o ambiente virtual e ativar**

```bash
python3 -m venv venv
source venv/bin/activate
```
(Você vai rodar `source venv/bin/activate` toda vez que abrir um terminal novo para
trabalhar neste projeto.)

- [ ] **Passo 3: Criar `requirements.txt` na raiz**

```
pandas
great_expectations==1.19.0
google-cloud-bigquery
google-cloud-storage
google-cloud-pubsub
gcsfs
db-dtypes
functions-framework
pytest
```

- [ ] **Passo 4: Instalar as dependências**

```bash
pip install -r requirements.txt
```

- [ ] **Passo 5: Criar a branch e commitar a estrutura**

```bash
git checkout -b chore/setup-gcp-and-repo
git add pipeline tests requirements.txt
git commit -m "chore: estrutura inicial de pastas e dependências da pipeline"
```

- [ ] **Passo 6: Abrir a Pull Request**

```bash
git push -u origin chore/setup-gcp-and-repo
gh pr create --title "chore: setup do GCP e estrutura do repositório" --body "Cria projeto GCP, bucket/dataset/tópico, e a estrutura inicial de pastas do repositório."
```
Revise a PR no GitHub e faça o merge para `main` antes de seguir para a Fase 1.

```bash
git checkout main
git pull origin main
```

---

## Fase 1 — Funções compartilhadas (Bronze/Silver/Gold usam as mesmas)

**Branch:** `feature/common-utils`

```bash
git checkout -b feature/common-utils
```

### Task 5: Escrever e testar `pipeline/common/gcs_utils.py`

**O que:** duas funções para gravar/ler Parquet no GCS, mais dois "atalhos" que
seguem a convenção de camada (dt=/latest/) descrita nas Global Constraints.

**Por quê:** toda Cloud Function precisa gravar e ler arquivos do GCS — em vez de
repetir essa lógica em cada uma, escrevemos e testamos uma vez aqui, e cada função
recebe uma cópia pronta.

- [ ] **Passo 1: Escrever o teste primeiro**

Crie `tests/test_gcs_utils.py`:
```python
import pandas as pd
import pipeline.common.gcs_utils as gcs_utils


def test_write_parquet_to_gcs_monta_uri_correta(monkeypatch):
    uris_chamadas = []

    def fake_to_parquet(self, path, index=False):
        uris_chamadas.append(path)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)
    df = pd.DataFrame({"a": [1, 2]})

    gcs_utils.write_parquet_to_gcs(df, "meu-bucket", "bronze/uf/uf.parquet")

    assert uris_chamadas == ["gs://meu-bucket/bronze/uf/uf.parquet"]


def test_read_parquet_from_gcs_monta_uri_correta(monkeypatch):
    uris_chamadas = []

    def fake_read_parquet(path):
        uris_chamadas.append(path)
        return pd.DataFrame({"a": [1]})

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet)

    df = gcs_utils.read_parquet_from_gcs("meu-bucket", "bronze/uf/uf.parquet")

    assert uris_chamadas == ["gs://meu-bucket/bronze/uf/uf.parquet"]
    assert len(df) == 1


def test_write_camada_parquet_grava_duas_copias(monkeypatch):
    chamadas = []

    def fake_write(df, bucket_name, blob_path):
        chamadas.append((bucket_name, blob_path))

    monkeypatch.setattr(gcs_utils, "write_parquet_to_gcs", fake_write)
    df = pd.DataFrame({"a": [1]})

    gcs_utils.write_camada_parquet(df, "meu-bucket", "bronze", "uf", data_referencia="2026-07-14")

    assert ("meu-bucket", "bronze/uf/dt=2026-07-14/uf.parquet") in chamadas
    assert ("meu-bucket", "bronze/uf/latest/uf.parquet") in chamadas


def test_read_camada_latest_le_o_caminho_latest(monkeypatch):
    caminhos_lidos = []

    def fake_read(bucket_name, blob_path):
        caminhos_lidos.append(blob_path)
        return pd.DataFrame({"a": [1]})

    monkeypatch.setattr(gcs_utils, "read_parquet_from_gcs", fake_read)

    gcs_utils.read_camada_latest("meu-bucket", "bronze", "uf")

    assert caminhos_lidos == ["bronze/uf/latest/uf.parquet"]
```

- [ ] **Passo 2: Rodar o teste e confirmar que falha**

```bash
pytest tests/test_gcs_utils.py -v
```
Expected: `ModuleNotFoundError` ou `ImportError` (o arquivo `gcs_utils.py` ainda não
existe).

- [ ] **Passo 3: Criar `pipeline/common/__init__.py` (vazio) e implementar**

```bash
touch pipeline/__init__.py pipeline/common/__init__.py
```

Crie `pipeline/common/gcs_utils.py`:
```python
from datetime import date

import pandas as pd


def write_parquet_to_gcs(df: pd.DataFrame, bucket_name: str, blob_path: str) -> None:
    uri = f"gs://{bucket_name}/{blob_path}"
    df.to_parquet(uri, index=False)


def read_parquet_from_gcs(bucket_name: str, blob_path: str) -> pd.DataFrame:
    uri = f"gs://{bucket_name}/{blob_path}"
    return pd.read_parquet(uri)


def write_camada_parquet(
    df: pd.DataFrame,
    bucket_name: str,
    camada: str,
    tabela: str,
    data_referencia: str | None = None,
) -> None:
    """Grava df em duas cópias: uma versionada por data (histórico) e uma 'latest'.

    A cópia versionada preserva o histórico completo (exigido para a camada Bronze);
    a cópia 'latest' permite que a próxima camada sempre leia o dado mais recente
    sem precisar descobrir qual é a partição de data mais nova.
    """
    data_referencia = data_referencia or date.today().isoformat()
    write_parquet_to_gcs(df, bucket_name, f"{camada}/{tabela}/dt={data_referencia}/{tabela}.parquet")
    write_parquet_to_gcs(df, bucket_name, f"{camada}/{tabela}/latest/{tabela}.parquet")


def read_camada_latest(bucket_name: str, camada: str, tabela: str) -> pd.DataFrame:
    return read_parquet_from_gcs(bucket_name, f"{camada}/{tabela}/latest/{tabela}.parquet")
```

- [ ] **Passo 4: Rodar o teste e confirmar que passa**

```bash
pytest tests/test_gcs_utils.py -v
```
Expected: `4 passed`.

- [ ] **Passo 5: Commit**

```bash
git add pipeline/__init__.py pipeline/common/__init__.py pipeline/common/gcs_utils.py tests/test_gcs_utils.py
git commit -m "feat: funções de leitura/escrita de Parquet no GCS com convenção de camadas"
```

---

### Task 6: Escrever e testar `pipeline/common/bq_utils.py`

**O que:** uma função para rodar uma query no BigQuery e devolver um DataFrame, e
outra para carregar um DataFrame de volta como tabela no BigQuery (usada pela camada
Gold).

**Por quê:** ingest_batch e ingest_stream sempre buscam dados via SQL, process_gold
sempre grava resultado final no BigQuery. Centralizamos essa lógica aqui.

- [ ] **Passo 1: Escrever o teste primeiro**

Crie `tests/test_bq_utils.py`:
```python
import pandas as pd
import pipeline.common.bq_utils as bq_utils


class FakeQueryJob:
    def __init__(self, df):
        self._df = df

    def to_dataframe(self):
        return self._df


class FakeBQClient:
    def __init__(self, project=None):
        self.project = project
        self.queries_recebidas = []
        self.cargas_recebidas = []

    def query(self, sql):
        self.queries_recebidas.append(sql)
        return FakeQueryJob(pd.DataFrame({"a": [1, 2]}))

    def load_table_from_dataframe(self, df, destino, job_config=None):
        self.cargas_recebidas.append((df, destino, job_config))

        class FakeLoadJob:
            def result(self_inner):
                return None

        return FakeLoadJob()


def test_query_public_table_roda_o_sql_recebido(monkeypatch):
    fake_client = FakeBQClient()
    monkeypatch.setattr(bq_utils.bigquery, "Client", lambda project=None: fake_client)

    df = bq_utils.query_public_table("SELECT * FROM tabela", project_id="meu-projeto")

    assert fake_client.queries_recebidas == ["SELECT * FROM tabela"]
    assert len(df) == 2


def test_load_dataframe_to_bq_monta_destino_correto(monkeypatch):
    fake_client = FakeBQClient()
    monkeypatch.setattr(bq_utils.bigquery, "Client", lambda project=None: fake_client)

    df = pd.DataFrame({"a": [1]})
    bq_utils.load_dataframe_to_bq(df, "meu-projeto", "gold_alfabetizacao", "indicador_por_municipio")

    assert len(fake_client.cargas_recebidas) == 1
    df_carregado, destino, _ = fake_client.cargas_recebidas[0]
    assert destino == "meu-projeto.gold_alfabetizacao.indicador_por_municipio"
```

- [ ] **Passo 2: Rodar e confirmar que falha**

```bash
pytest tests/test_bq_utils.py -v
```
Expected: `ModuleNotFoundError`.

- [ ] **Passo 3: Implementar**

Crie `pipeline/common/bq_utils.py`:
```python
from google.cloud import bigquery


def query_public_table(sql: str, project_id: str) -> "pandas.DataFrame":
    client = bigquery.Client(project=project_id)
    return client.query(sql).to_dataframe()


def load_dataframe_to_bq(
    df: "pandas.DataFrame",
    project_id: str,
    dataset_id: str,
    table_id: str,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    client = bigquery.Client(project=project_id)
    destino = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = client.load_table_from_dataframe(df, destino, job_config=job_config)
    job.result()
```

- [ ] **Passo 4: Rodar e confirmar que passa**

```bash
pytest tests/test_bq_utils.py -v
```
Expected: `2 passed`.

- [ ] **Passo 5: Commit, push e PR**

```bash
git add pipeline/common/bq_utils.py tests/test_bq_utils.py
git commit -m "feat: funções de query e carga no BigQuery"
git push -u origin feature/common-utils
gh pr create --title "feat: funções compartilhadas de GCS e BigQuery" --body "Adiciona e testa as funções usadas por todas as Cloud Functions da pipeline."
```
Revise e faça merge no GitHub, depois:
```bash
git checkout main && git pull origin main
```

---

## Fase 2 — Bronze: ingestão Batch

**Branch:** `feature/bronze-ingestion`

```bash
git checkout -b feature/bronze-ingestion
```

### Task 7: Escrever e testar `pipeline/ingest_batch/main.py`

**O que:** uma Cloud Function acionada por HTTP (o Cloud Scheduler vai chamá-la) que
roda uma query para cada uma das 8 tabelas fonte (as 6 exigidas pelo desafio + a
tabela `dicionario` + a tabela de geografia) e grava cada resultado como Parquet no
bronze.

**Por quê:** essa é a etapa de ingestão histórica/estrutural da arquitetura híbrida —
o "batch" da PDF do desafio.

**Nota de FinOps:** cada query abaixo lista só as colunas realmente usadas nas
próximas camadas, em vez de `SELECT *`. O BigQuery cobra por bytes de coluna
escaneados, então evitar colunas desnecessárias (ex.: `caderno`,
`preenchimento_caderno` da tabela `alunos`, que são metadados de logística de prova,
não usados em nenhuma análise) já é uma otimização de custo real, não só estilo.

- [ ] **Passo 1: Escrever o teste primeiro**

Crie `tests/test_ingest_batch.py`:
```python
import pandas as pd
from pipeline.ingest_batch import main as ingest_batch


def test_ingest_batch_grava_todas_as_tabelas(monkeypatch):
    queries_rodadas = []
    tabelas_gravadas = []

    def fake_query_public_table(sql, project_id):
        queries_rodadas.append(sql)
        return pd.DataFrame({"a": [1]})

    def fake_write_camada_parquet(df, bucket_name, camada, tabela, data_referencia=None):
        tabelas_gravadas.append((camada, tabela))

    monkeypatch.setattr(ingest_batch, "query_public_table", fake_query_public_table)
    monkeypatch.setattr(ingest_batch, "write_camada_parquet", fake_write_camada_parquet)
    monkeypatch.setenv("BUCKET_NAME", "meu-bucket")
    monkeypatch.setenv("PROJECT_ID", "meu-projeto")

    resultado, status = ingest_batch.ingest_batch(request=None)

    assert status == 200
    assert len(queries_rodadas) == len(ingest_batch.TABELAS_BATCH)
    nomes_gravados = {tabela for _, tabela in tabelas_gravadas}
    assert nomes_gravados == set(ingest_batch.TABELAS_BATCH.keys())
    assert all(camada == "bronze" for camada, _ in tabelas_gravadas)
```

- [ ] **Passo 2: Rodar e confirmar que falha**

```bash
pytest tests/test_ingest_batch.py -v
```
Expected: `ModuleNotFoundError`.

- [ ] **Passo 3: Implementar**

Crie `pipeline/ingest_batch/__init__.py` (vazio) e `pipeline/ingest_batch/main.py`:
```python
import os

import functions_framework

from pipeline.common.bq_utils import query_public_table
from pipeline.common.gcs_utils import write_camada_parquet

TABELAS_BATCH = {
    "uf": """
        SELECT ano, sigla_uf, serie, rede, taxa_alfabetizacao, media_portugues,
               proporcao_aluno_nivel_0, proporcao_aluno_nivel_1, proporcao_aluno_nivel_2,
               proporcao_aluno_nivel_3, proporcao_aluno_nivel_4, proporcao_aluno_nivel_5,
               proporcao_aluno_nivel_6, proporcao_aluno_nivel_7, proporcao_aluno_nivel_8
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf`
    """,
    "municipio": """
        SELECT ano, id_municipio, serie, rede, taxa_alfabetizacao, media_portugues,
               proporcao_aluno_nivel_0, proporcao_aluno_nivel_1, proporcao_aluno_nivel_2,
               proporcao_aluno_nivel_3, proporcao_aluno_nivel_4, proporcao_aluno_nivel_5,
               proporcao_aluno_nivel_6, proporcao_aluno_nivel_7, proporcao_aluno_nivel_8
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.municipio`
    """,
    "meta_alfabetizacao_brasil": """
        SELECT ano, rede, taxa_alfabetizacao, meta_alfabetizacao_2024, meta_alfabetizacao_2025,
               meta_alfabetizacao_2026, meta_alfabetizacao_2027, meta_alfabetizacao_2028,
               meta_alfabetizacao_2029, meta_alfabetizacao_2030, percentual_participacao
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil`
    """,
    "meta_alfabetizacao_uf": """
        SELECT ano, sigla_uf, rede, taxa_alfabetizacao, meta_alfabetizacao_2024, meta_alfabetizacao_2025,
               meta_alfabetizacao_2026, meta_alfabetizacao_2027, meta_alfabetizacao_2028,
               meta_alfabetizacao_2029, meta_alfabetizacao_2030, percentual_participacao
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf`
    """,
    "meta_alfabetizacao_municipio": """
        SELECT ano, id_municipio, rede, taxa_alfabetizacao, meta_alfabetizacao_2024, meta_alfabetizacao_2025,
               meta_alfabetizacao_2026, meta_alfabetizacao_2027, meta_alfabetizacao_2028,
               meta_alfabetizacao_2029, meta_alfabetizacao_2030, nivel_alfabetizacao, percentual_participacao
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio`
    """,
    "alunos": """
        SELECT ano, id_municipio, id_escola, id_aluno, serie, rede, presenca,
               alfabetizado, proficiencia, peso_aluno
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.alunos`
    """,
    "dicionario": """
        SELECT id_tabela, nome_coluna, chave, cobertura_temporal, valor
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    """,
    "municipio_geo": """
        SELECT id_municipio, nome, sigla_uf, nome_uf, id_uf
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    """,
}


@functions_framework.http
def ingest_batch(request):
    bucket_name = os.environ["BUCKET_NAME"]
    project_id = os.environ["PROJECT_ID"]

    linhas_por_tabela = {}
    for tabela, sql in TABELAS_BATCH.items():
        df = query_public_table(sql, project_id=project_id)
        write_camada_parquet(df, bucket_name, "bronze", tabela)
        linhas_por_tabela[tabela] = len(df)

    return linhas_por_tabela, 200
```

- [ ] **Passo 4: Rodar e confirmar que passa**

```bash
pytest tests/test_ingest_batch.py -v
```
Expected: `1 passed`.

- [ ] **Passo 5: Commit**

```bash
git add pipeline/ingest_batch tests/test_ingest_batch.py
git commit -m "feat: Cloud Function ingest_batch para as 8 tabelas fonte"
```

---

### Task 8: Preparar a pasta de deploy e publicar a Cloud Function `ingest-batch`

**O que:** copiar os arquivos compartilhados para dentro da pasta da função (porque
o deploy é isolado por pasta, como explicado nas Global Constraints), criar o
`requirements.txt` da função, e rodar o comando de deploy.

**Por quê:** até aqui só testamos localmente com funções "fake" (monkeypatch). Este
passo coloca a função de verdade rodando no GCP.

- [ ] **Passo 1: Copiar os arquivos compartilhados**

```bash
cp pipeline/common/gcs_utils.py pipeline/ingest_batch/gcs_utils.py
cp pipeline/common/bq_utils.py pipeline/ingest_batch/bq_utils.py
```

- [ ] **Passo 2: Ajustar os imports em `pipeline/ingest_batch/main.py`**

Troque:
```python
from pipeline.common.bq_utils import query_public_table
from pipeline.common.gcs_utils import write_camada_parquet
```
por:
```python
from bq_utils import query_public_table
from gcs_utils import write_camada_parquet
```
(Dentro da pasta de deploy os arquivos são "vizinhos", sem o pacote `pipeline.common`.)

- [ ] **Passo 3: Criar `pipeline/ingest_batch/requirements.txt`**

```
pandas
google-cloud-bigquery
google-cloud-storage
gcsfs
db-dtypes
functions-framework
```

- [ ] **Passo 4: Fazer o deploy**

```bash
gcloud functions deploy ingest-batch \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=pipeline/ingest_batch \
  --entry-point=ingest_batch \
  --trigger-http \
  --allow-unauthenticated \
  --memory=512Mi \
  --timeout=300s \
  --set-env-vars=BUCKET_NAME=SEU_PROJECT_ID-pipeline-alfabetizacao,PROJECT_ID=SEU_PROJECT_ID
```
Esse comando demora alguns minutos na primeira vez (o Cloud Build compila a imagem).
Expected, no final: `url: https://...` (a URL da função).

> Nota de segurança: usamos `--allow-unauthenticated` para simplificar (o Cloud
> Scheduler chama a função sem precisar de token). Como os dados são públicos e o
> projeto é de estudo, esse é um trade-off aceitável — mencione isso no README como
> uma simplificação consciente, não algo que passou despercebido.

- [ ] **Passo 5: Testar manualmente**

```bash
FUNCTION_URL=$(gcloud functions describe ingest-batch --gen2 --region=us-central1 --format='value(serviceConfig.uri)')
curl "$FUNCTION_URL"
```
Expected: um JSON com a contagem de linhas de cada tabela, ex.:
`{"uf": 135, "municipio": 27850, ...}`.

- [ ] **Passo 6: Verificar no bucket**

```bash
gcloud storage ls gs://SEU_PROJECT_ID-pipeline-alfabetizacao/bronze/
```
Expected: uma pasta por tabela (`uf/`, `municipio/`, etc.), cada uma com `dt=.../` e
`latest/`.

---

### Task 9: Criar o job do Cloud Scheduler para `ingest-batch`

**O que:** agendar a função para rodar automaticamente todo dia às 3h da manhã.

**Por quê:** é isso que torna a ingestão "batch periódica" de verdade, em vez de só
manual — o requisito do desafio pede processamento periódico agendado.

- [ ] **Passo 1: Criar o job**

```bash
FUNCTION_URL=$(gcloud functions describe ingest-batch --gen2 --region=us-central1 --format='value(serviceConfig.uri)')
gcloud scheduler jobs create http job-ingest-batch \
  --location=us-central1 \
  --schedule="0 3 * * *" \
  --uri="$FUNCTION_URL" \
  --http-method=GET
```
Expected: `Created job [job-ingest-batch].`

- [ ] **Passo 2: Forçar uma execução manual para testar o job (sem esperar 3h)**

```bash
gcloud scheduler jobs run job-ingest-batch --location=us-central1
```

- [ ] **Passo 3: Verificar no console de logs**

```bash
gcloud functions logs read ingest-batch --gen2 --region=us-central1 --limit=20
```
Expected: logs de execução recente sem erro.

- [ ] **Passo 4: Commit e PR**

```bash
git add pipeline/ingest_batch
git commit -m "feat: deploy da Cloud Function ingest-batch com agendamento diário"
git push -u origin feature/bronze-ingestion
gh pr create --title "feat: ingestão batch (Bronze) das 8 tabelas fonte" --body "Implementa e implanta a Cloud Function ingest-batch, agendada diariamente via Cloud Scheduler."
```
Revise e faça merge, depois:
```bash
git checkout main && git pull origin main
```

---

## Fase 3 — Streaming (simulação de eventos)

**Branch:** `feature/streaming`

```bash
git checkout -b feature/streaming
```

### Task 10: Escrever e testar `scripts/publish_stream_events.py`

**O que:** um script que gera eventos simulados (ex.: "a taxa de alfabetização de SP
foi atualizada para 87%") e publica no tópico Pub/Sub.

**Por quê:** o desafio pede a simulação de "ingestão de eventos em tempo quase real"
— não existe uma fonte real que emita esses eventos ao vivo, então simulamos.

- [ ] **Passo 1: Escrever o teste primeiro (só a parte pura, sem publicar de verdade)**

Crie `tests/test_publish_stream_events.py`:
```python
from scripts.publish_stream_events import gerar_evento_simulado, UFS


def test_gerar_evento_simulado_tem_os_campos_esperados():
    evento = gerar_evento_simulado()

    assert set(evento.keys()) == {"tipo_evento", "ano", "sigla_uf", "taxa_alfabetizacao", "timestamp"}
    assert evento["tipo_evento"] == "atualizacao_indicador"
    assert evento["sigla_uf"] in UFS
    assert 0 <= evento["taxa_alfabetizacao"] <= 100
```

- [ ] **Passo 2: Rodar e confirmar que falha**

```bash
touch scripts/__init__.py
pytest tests/test_publish_stream_events.py -v
```
Expected: `ModuleNotFoundError`.

- [ ] **Passo 3: Implementar**

Crie `scripts/publish_stream_events.py`:
```python
import json
import os
import random
import time
from datetime import date

from google.cloud import pubsub_v1

UFS = ["SP", "RJ", "MG", "BA", "CE", "PA", "PR", "RS", "PE", "AM"]


def gerar_evento_simulado() -> dict:
    return {
        "tipo_evento": "atualizacao_indicador",
        "ano": date.today().year,
        "sigla_uf": random.choice(UFS),
        "taxa_alfabetizacao": round(random.uniform(60, 95), 2),
        "timestamp": time.time(),
    }


def publicar_eventos(project_id: str, topico: str, quantidade: int = 5, intervalo_segundos: int = 2) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topico)

    for _ in range(quantidade):
        evento = gerar_evento_simulado()
        data = json.dumps(evento).encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(f"Publicado: {evento} -> message_id={future.result()}")
        time.sleep(intervalo_segundos)


if __name__ == "__main__":
    publicar_eventos(
        project_id=os.environ["PROJECT_ID"],
        topico="alfabetizacao-eventos",
    )
```

- [ ] **Passo 4: Rodar e confirmar que passa**

```bash
pytest tests/test_publish_stream_events.py -v
```
Expected: `1 passed`.

- [ ] **Passo 5: Commit**

```bash
git add scripts/__init__.py scripts/publish_stream_events.py tests/test_publish_stream_events.py
git commit -m "feat: script publicador de eventos simulados no Pub/Sub"
```

---

### Task 11: Escrever e testar `pipeline/ingest_stream/main.py`

**O que:** uma Cloud Function acionada automaticamente sempre que uma mensagem chega
no tópico Pub/Sub. Ela decodifica o evento e grava no bronze.

**Por quê:** é o lado "consumidor" do streaming — fecha o ciclo publisher → tópico →
consumer que demonstra a ingestão orientada a evento (diferente do batch, que é
orientado a agenda/tempo).

- [ ] **Passo 1: Escrever o teste primeiro**

Crie `tests/test_ingest_stream.py`:
```python
import base64
import json

from pipeline.ingest_stream import main as ingest_stream


class FakeCloudEvent:
    def __init__(self, payload: dict):
        mensagem_codificada = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
        self.data = {"message": {"data": mensagem_codificada}}


def test_ingest_stream_grava_o_evento_recebido(monkeypatch):
    tabelas_gravadas = []

    def fake_write_camada_parquet(df, bucket_name, camada, tabela, data_referencia=None):
        tabelas_gravadas.append((camada, tabela, df.to_dict("records")))

    monkeypatch.setattr(ingest_stream, "write_camada_parquet", fake_write_camada_parquet)
    monkeypatch.setenv("BUCKET_NAME", "meu-bucket")

    evento = {
        "tipo_evento": "atualizacao_indicador",
        "ano": 2026,
        "sigla_uf": "SP",
        "taxa_alfabetizacao": 88.5,
        "timestamp": 123.0,
    }
    cloud_event = FakeCloudEvent(evento)

    ingest_stream.ingest_stream(cloud_event)

    assert len(tabelas_gravadas) == 1
    camada, tabela, linhas = tabelas_gravadas[0]
    assert camada == "bronze"
    assert tabela == "eventos_streaming"
    assert linhas[0]["sigla_uf"] == "SP"
```

- [ ] **Passo 2: Rodar e confirmar que falha**

```bash
pytest tests/test_ingest_stream.py -v
```
Expected: `ModuleNotFoundError`.

- [ ] **Passo 3: Implementar**

Crie `pipeline/ingest_stream/__init__.py` (vazio) e `pipeline/ingest_stream/main.py`:
```python
import base64
import json
import os
from datetime import datetime

import functions_framework
import pandas as pd

from pipeline.common.gcs_utils import write_camada_parquet


@functions_framework.cloud_event
def ingest_stream(cloud_event):
    bucket_name = os.environ["BUCKET_NAME"]

    mensagem_codificada = cloud_event.data["message"]["data"]
    payload = base64.b64decode(mensagem_codificada).decode("utf-8")
    evento = json.loads(payload)

    df = pd.DataFrame([evento])
    agora = datetime.utcnow()
    write_camada_parquet(
        df,
        bucket_name,
        "bronze",
        "eventos_streaming",
        data_referencia=agora.strftime("%Y-%m-%dT%H%M%S"),
    )
```

- [ ] **Passo 4: Rodar e confirmar que passa**

```bash
pytest tests/test_ingest_stream.py -v
```
Expected: `1 passed`.

- [ ] **Passo 5: Commit**

```bash
git add pipeline/ingest_stream tests/test_ingest_stream.py
git commit -m "feat: Cloud Function ingest_stream acionada por Pub/Sub"
```

---

### Task 12: Fazer o deploy de `ingest-stream` com trigger no Pub/Sub

**O que:** igual à Task 8, mas para a função de streaming, e com trigger de Pub/Sub
em vez de HTTP.

- [ ] **Passo 1: Copiar os arquivos compartilhados e ajustar imports**

```bash
cp pipeline/common/gcs_utils.py pipeline/ingest_stream/gcs_utils.py
```
Em `pipeline/ingest_stream/main.py`, troque:
```python
from pipeline.common.gcs_utils import write_camada_parquet
```
por:
```python
from gcs_utils import write_camada_parquet
```

- [ ] **Passo 2: Criar `pipeline/ingest_stream/requirements.txt`**

```
pandas
google-cloud-storage
gcsfs
db-dtypes
functions-framework
```

- [ ] **Passo 3: Deploy**

```bash
gcloud functions deploy ingest-stream \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=pipeline/ingest_stream \
  --entry-point=ingest_stream \
  --trigger-topic=alfabetizacao-eventos \
  --memory=256Mi \
  --timeout=60s \
  --set-env-vars=BUCKET_NAME=SEU_PROJECT_ID-pipeline-alfabetizacao
```

- [ ] **Passo 4: Testar publicando eventos de verdade**

```bash
export PROJECT_ID=SEU_PROJECT_ID
python3 scripts/publish_stream_events.py
```
Expected: 5 linhas `Publicado: {...} -> message_id=...` no terminal.

- [ ] **Passo 5: Verificar no bucket e nos logs**

```bash
gcloud storage ls gs://SEU_PROJECT_ID-pipeline-alfabetizacao/bronze/eventos_streaming/latest/
gcloud functions logs read ingest-stream --gen2 --region=us-central1 --limit=20
```
Expected: um arquivo `eventos_streaming.parquet` e logs sem erro para cada uma das 5
mensagens publicadas.

- [ ] **Passo 6: Commit, push e PR**

```bash
git add pipeline/ingest_stream
git commit -m "feat: deploy da Cloud Function ingest-stream com trigger Pub/Sub"
git push -u origin feature/streaming
gh pr create --title "feat: ingestão streaming simulada (Pub/Sub)" --body "Implementa o publisher de eventos simulados e a Cloud Function consumidora acionada por Pub/Sub."
```
Revise e faça merge, depois:
```bash
git checkout main && git pull origin main
```

---

## Fase 4 — Silver: limpeza, qualidade e integração

**Branch:** `feature/silver-quality`

```bash
git checkout -b feature/silver-quality
```

### Task 13: Escrever e testar `pipeline/process_silver/transform.py`

**O que:** funções puras de limpeza (remover duplicatas, preencher nulos, padronizar
o formato do código do município) e de integração (juntar resultado com meta, juntar
com nome/UF do município).

**Por quê:** são exatamente as 5 transformações que o desafio pede na camada Silver:
limpeza, tratamento de nulos, padronização de tipos, normalização de chaves, e
integração das bases.

- [ ] **Passo 1: Escrever os testes primeiro**

Crie `tests/test_silver_transform.py`:
```python
import pandas as pd

from pipeline.process_silver.transform import (
    enriquecer_com_geografia,
    integrar_resultado_com_meta,
    padronizar_id_municipio,
    preencher_valores_ausentes_numericos,
    remover_linhas_duplicadas,
)


def test_remover_linhas_duplicadas_mantem_a_primeira_ocorrencia():
    df = pd.DataFrame({
        "ano": [2023, 2023, 2024],
        "id_municipio": ["3550308", "3550308", "3550308"],
        "rede": ["municipal", "municipal", "municipal"],
        "taxa_alfabetizacao": [85.0, 99.0, 70.0],
    })

    resultado = remover_linhas_duplicadas(df, colunas_chave=["ano", "id_municipio", "rede"])

    assert len(resultado) == 2
    assert resultado.loc[resultado["ano"] == 2023, "taxa_alfabetizacao"].iloc[0] == 85.0


def test_preencher_valores_ausentes_numericos_usa_a_mediana():
    df = pd.DataFrame({"taxa_alfabetizacao": [80.0, None, 90.0]})

    resultado = preencher_valores_ausentes_numericos(df, colunas_numericas=["taxa_alfabetizacao"])

    assert resultado["taxa_alfabetizacao"].isna().sum() == 0
    assert resultado["taxa_alfabetizacao"].iloc[1] == 85.0


def test_padronizar_id_municipio_preenche_com_zeros_a_esquerda():
    df = pd.DataFrame({"id_municipio": ["355030", 3550308, " 3550308 "]})

    resultado = padronizar_id_municipio(df)

    assert resultado["id_municipio"].tolist() == ["0355030", "3550308", "3550308"]


def test_enriquecer_com_geografia_junta_nome_e_uf():
    df_indicador = pd.DataFrame({"id_municipio": ["3550308"], "taxa_alfabetizacao": [85.0]})
    df_geo = pd.DataFrame({
        "id_municipio": ["3550308"],
        "nome": ["São Paulo"],
        "sigla_uf": ["SP"],
        "nome_uf": ["São Paulo"],
    })

    resultado = enriquecer_com_geografia(df_indicador, df_geo)

    assert resultado.loc[0, "nome"] == "São Paulo"
    assert resultado.loc[0, "sigla_uf"] == "SP"


def test_integrar_resultado_com_meta_junta_pelas_chaves():
    df_resultado = pd.DataFrame({
        "ano": [2024], "id_municipio": ["3550308"], "rede": ["municipal"], "taxa_alfabetizacao": [85.0],
    })
    df_meta = pd.DataFrame({
        "ano": [2024], "id_municipio": ["3550308"], "rede": ["municipal"], "meta_alfabetizacao_2024": [90.0],
    })

    resultado = integrar_resultado_com_meta(df_resultado, df_meta, chaves=["ano", "id_municipio", "rede"])

    assert resultado.loc[0, "meta_alfabetizacao_2024"] == 90.0
```

- [ ] **Passo 2: Rodar e confirmar que falha**

```bash
pytest tests/test_silver_transform.py -v
```
Expected: `ModuleNotFoundError`.

- [ ] **Passo 3: Implementar**

Crie `pipeline/process_silver/__init__.py` (vazio) e
`pipeline/process_silver/transform.py`:
```python
import pandas as pd


def remover_linhas_duplicadas(df: pd.DataFrame, colunas_chave: list) -> pd.DataFrame:
    return df.drop_duplicates(subset=colunas_chave, keep="first").reset_index(drop=True)


def preencher_valores_ausentes_numericos(df: pd.DataFrame, colunas_numericas: list) -> pd.DataFrame:
    df = df.copy()
    for coluna in colunas_numericas:
        df[coluna] = df[coluna].fillna(df[coluna].median())
    return df


def padronizar_id_municipio(df: pd.DataFrame, coluna: str = "id_municipio") -> pd.DataFrame:
    df = df.copy()
    df[coluna] = df[coluna].astype(str).str.strip().str.zfill(7)
    return df


def enriquecer_com_geografia(df_indicador: pd.DataFrame, df_geo: pd.DataFrame) -> pd.DataFrame:
    return df_indicador.merge(
        df_geo[["id_municipio", "nome", "sigla_uf", "nome_uf"]],
        on="id_municipio",
        how="left",
    )


def integrar_resultado_com_meta(df_resultado: pd.DataFrame, df_meta: pd.DataFrame, chaves: list) -> pd.DataFrame:
    return df_resultado.merge(df_meta, on=chaves, how="left", suffixes=("", "_meta"))
```

- [ ] **Passo 4: Rodar e confirmar que passa**

```bash
pytest tests/test_silver_transform.py -v
```
Expected: `5 passed`.

- [ ] **Passo 5: Commit**

```bash
git add pipeline/process_silver/__init__.py pipeline/process_silver/transform.py tests/test_silver_transform.py
git commit -m "feat: funções de limpeza e integração da camada Silver"
```

---

### Task 14: Escrever e testar `pipeline/process_silver/quality.py` (Great Expectations)

**O que:** uma função que recebe um DataFrame e uma lista de "expectativas" (regras
de qualidade) e devolve se passou ou não, usando a biblioteca Great Expectations —
a mesma ferramenta usada no laboratório do curso.

**Por quê:** cobre as 4 checagens de qualidade exigidas pelo desafio: duplicidade,
valores ausentes, chaves de relacionamento (via unicidade composta) e consistência
(via faixa de valores válidos).

> Nota: a API abaixo (`gx.get_context(mode="ephemeral")`,
> `context.data_sources.add_pandas(...)`, etc.) foi testada e confirmada nesta sessão
> contra `great_expectations==1.19.0` instalado de verdade — não é código genérico.

- [ ] **Passo 1: Escrever o teste primeiro**

Crie `tests/test_silver_quality.py`:
```python
import pandas as pd

from pipeline.process_silver.quality import validar_dataframe


def test_validar_dataframe_detecta_dados_bons():
    df = pd.DataFrame({
        "ano": [2023, 2023],
        "id_municipio": ["3550308", "3304557"],
        "rede": ["municipal", "municipal"],
        "taxa_alfabetizacao": [85.0, 70.0],
    })

    resultado = validar_dataframe(
        df,
        nome_ativo="municipio_teste",
        colunas_nao_nulas=["id_municipio", "taxa_alfabetizacao"],
        colunas_chave_unica=["ano", "id_municipio", "rede"],
        coluna_faixa_valida=("taxa_alfabetizacao", 0, 100),
    )

    assert resultado["sucesso"] is True


def test_validar_dataframe_detecta_duplicidade_e_valor_fora_da_faixa():
    df = pd.DataFrame({
        "ano": [2023, 2023],
        "id_municipio": ["3550308", "3550308"],
        "rede": ["municipal", "municipal"],
        "taxa_alfabetizacao": [85.0, 150.0],
    })

    resultado = validar_dataframe(
        df,
        nome_ativo="municipio_teste_ruim",
        colunas_nao_nulas=["id_municipio", "taxa_alfabetizacao"],
        colunas_chave_unica=["ano", "id_municipio", "rede"],
        coluna_faixa_valida=("taxa_alfabetizacao", 0, 100),
    )

    assert resultado["sucesso"] is False
    assert resultado["detalhes"]["expect_compound_columns_to_be_unique"] is False
    assert resultado["detalhes"]["expect_column_values_to_be_between"] is False
```

- [ ] **Passo 2: Rodar e confirmar que falha**

```bash
pytest tests/test_silver_quality.py -v
```
Expected: `ModuleNotFoundError`.

- [ ] **Passo 3: Implementar**

Crie `pipeline/process_silver/quality.py`:
```python
import great_expectations as gx
import pandas as pd


def validar_dataframe(
    df: pd.DataFrame,
    nome_ativo: str,
    colunas_nao_nulas: list,
    colunas_chave_unica: list,
    coluna_faixa_valida: tuple,
) -> dict:
    """Roda checagens de qualidade sobre um DataFrame usando Great Expectations.

    Cobre os 4 pontos exigidos pelo desafio: valores ausentes (colunas_nao_nulas),
    duplicidade/chave de relacionamento (colunas_chave_unica, checada como unicidade
    composta) e consistência (coluna_faixa_valida, ex.: percentuais entre 0 e 100).
    """
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(f"{nome_ativo}_datasource")
    data_asset = data_source.add_dataframe_asset(name=nome_ativo)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{nome_ativo}_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = gx.ExpectationSuite(name=f"{nome_ativo}_suite")
    for coluna in colunas_nao_nulas:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=coluna))
    suite.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=colunas_chave_unica))

    coluna_faixa, valor_min, valor_max = coluna_faixa_valida
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column=coluna_faixa, min_value=valor_min, max_value=valor_max)
    )

    resultado = batch.validate(suite)

    detalhes = {r.expectation_config.type: r.success for r in resultado.results}
    return {"sucesso": resultado.success, "detalhes": detalhes}
```

- [ ] **Passo 4: Rodar e confirmar que passa**

```bash
pytest tests/test_silver_quality.py -v
```
Expected: `2 passed`.

- [ ] **Passo 5: Commit**

```bash
git add pipeline/process_silver/quality.py tests/test_silver_quality.py
git commit -m "feat: validação de qualidade de dados com Great Expectations"
```

---

### Task 15: Escrever `pipeline/process_silver/main.py` (Cloud Function de junção)

**O que:** a função que lê os dados do bronze, aplica limpeza, valida qualidade, e
só grava no silver se a validação passar. Produz 4 datasets: `municipio_integrado`,
`uf_integrado`, `brasil_integrado`, `alunos_limpo`.

**Por quê:** é aqui que a "integração das bases" pedida pelo desafio acontece de
fato — cada dataset final junta resultado real + meta + (quando aplicável)
geografia.

- [ ] **Passo 1: Escrever o teste primeiro**

Crie `tests/test_process_silver.py`:
```python
import pandas as pd

from pipeline.process_silver import main as process_silver


def _bronze_fake(nome_tabela):
    dados = {
        "uf": pd.DataFrame({
            "ano": [2024], "sigla_uf": ["SP"], "rede": ["municipal"], "taxa_alfabetizacao": [85.0],
        }),
        "municipio": pd.DataFrame({
            "ano": [2024], "id_municipio": ["3550308"], "rede": ["municipal"], "taxa_alfabetizacao": [85.0],
        }),
        "meta_alfabetizacao_brasil": pd.DataFrame({
            "ano": [2024], "rede": ["municipal"], "taxa_alfabetizacao": [80.0], "meta_alfabetizacao_2024": [82.0],
        }),
        "meta_alfabetizacao_uf": pd.DataFrame({
            "ano": [2024], "sigla_uf": ["SP"], "rede": ["municipal"], "meta_alfabetizacao_2024": [88.0],
        }),
        "meta_alfabetizacao_municipio": pd.DataFrame({
            "ano": [2024], "id_municipio": ["3550308"], "rede": ["municipal"], "meta_alfabetizacao_2024": [90.0],
        }),
        "alunos": pd.DataFrame({
            "ano": [2024], "id_aluno": ["1"], "id_municipio": ["3550308"], "proficiencia": [750.0],
        }),
        "municipio_geo": pd.DataFrame({
            "id_municipio": ["3550308"], "nome": ["São Paulo"], "sigla_uf": ["SP"], "nome_uf": ["São Paulo"],
        }),
    }
    return dados[nome_tabela]


def test_process_silver_grava_os_quatro_datasets_integrados(monkeypatch):
    tabelas_gravadas = []

    monkeypatch.setattr(process_silver, "read_camada_latest", lambda bucket, camada, tabela: _bronze_fake(tabela))
    monkeypatch.setattr(
        process_silver,
        "write_camada_parquet",
        lambda df, bucket, camada, tabela, data_referencia=None: tabelas_gravadas.append(tabela),
    )
    monkeypatch.setattr(
        process_silver,
        "validar_dataframe",
        lambda *args, **kwargs: {"sucesso": True, "detalhes": {}},
    )
    monkeypatch.setenv("BUCKET_NAME", "meu-bucket")

    resultado, status = process_silver.process_silver(request=None)

    assert status == 200
    assert set(tabelas_gravadas) == {"municipio_integrado", "uf_integrado", "brasil_integrado", "alunos_limpo"}


def test_process_silver_retorna_erro_se_qualidade_falhar(monkeypatch):
    monkeypatch.setattr(process_silver, "read_camada_latest", lambda bucket, camada, tabela: _bronze_fake(tabela))
    monkeypatch.setattr(process_silver, "write_camada_parquet", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        process_silver,
        "validar_dataframe",
        lambda *args, **kwargs: {"sucesso": False, "detalhes": {"expect_column_values_to_not_be_null": False}},
    )
    monkeypatch.setenv("BUCKET_NAME", "meu-bucket")

    resultado, status = process_silver.process_silver(request=None)

    assert status == 500
```

- [ ] **Passo 2: Rodar e confirmar que falha**

```bash
pytest tests/test_process_silver.py -v
```
Expected: `ModuleNotFoundError` ou `AttributeError`.

- [ ] **Passo 3: Implementar**

Crie `pipeline/process_silver/main.py`:
```python
import os

import functions_framework

from pipeline.common.gcs_utils import read_camada_latest, write_camada_parquet
from pipeline.process_silver.quality import validar_dataframe
from pipeline.process_silver.transform import (
    enriquecer_com_geografia,
    integrar_resultado_com_meta,
    padronizar_id_municipio,
    preencher_valores_ausentes_numericos,
    remover_linhas_duplicadas,
)


@functions_framework.http
def process_silver(request):
    bucket_name = os.environ["BUCKET_NAME"]

    uf = read_camada_latest(bucket_name, "bronze", "uf")
    municipio = read_camada_latest(bucket_name, "bronze", "municipio")
    meta_brasil = read_camada_latest(bucket_name, "bronze", "meta_alfabetizacao_brasil")
    meta_uf = read_camada_latest(bucket_name, "bronze", "meta_alfabetizacao_uf")
    meta_municipio = read_camada_latest(bucket_name, "bronze", "meta_alfabetizacao_municipio")
    alunos = read_camada_latest(bucket_name, "bronze", "alunos")
    geo = read_camada_latest(bucket_name, "bronze", "municipio_geo")

    municipio = remover_linhas_duplicadas(municipio, ["ano", "id_municipio", "rede"])
    municipio = preencher_valores_ausentes_numericos(municipio, ["taxa_alfabetizacao"])
    municipio = padronizar_id_municipio(municipio)
    meta_municipio = padronizar_id_municipio(meta_municipio)
    municipio_integrado = integrar_resultado_com_meta(
        municipio, meta_municipio, chaves=["ano", "id_municipio", "rede"]
    )
    municipio_integrado = enriquecer_com_geografia(municipio_integrado, geo)

    uf = remover_linhas_duplicadas(uf, ["ano", "sigla_uf", "rede"])
    uf = preencher_valores_ausentes_numericos(uf, ["taxa_alfabetizacao"])
    uf_integrado = integrar_resultado_com_meta(uf, meta_uf, chaves=["ano", "sigla_uf", "rede"])

    brasil_integrado = remover_linhas_duplicadas(meta_brasil, ["ano", "rede"])
    brasil_integrado = preencher_valores_ausentes_numericos(brasil_integrado, ["taxa_alfabetizacao"])

    alunos_limpo = remover_linhas_duplicadas(alunos, ["ano", "id_aluno"])
    alunos_limpo = preencher_valores_ausentes_numericos(alunos_limpo, ["proficiencia"])
    alunos_limpo = padronizar_id_municipio(alunos_limpo)

    # Validamos com Great Expectations o dataset município (o mais granular e citado
    # explicitamente como exemplo de Gold no desafio). uf_integrado/brasil_integrado/
    # alunos_limpo já passam pela limpeza determinística acima (dedup + fillna), mas
    # não têm uma segunda checagem via GE — trade-off consciente de escopo/tempo, não
    # uma lacuna esquecida. Ampliar para os outros 3 datasets seria só repetir esta
    # mesma chamada com outras colunas/chaves, caso sobre tempo.
    validacao = validar_dataframe(
        municipio_integrado,
        nome_ativo="municipio_integrado",
        colunas_nao_nulas=["id_municipio", "taxa_alfabetizacao"],
        colunas_chave_unica=["ano", "id_municipio", "rede"],
        coluna_faixa_valida=("taxa_alfabetizacao", 0, 100),
    )
    if not validacao["sucesso"]:
        return {"erro": "validação de qualidade falhou", "detalhes": validacao["detalhes"]}, 500

    write_camada_parquet(municipio_integrado, bucket_name, "silver", "municipio_integrado")
    write_camada_parquet(uf_integrado, bucket_name, "silver", "uf_integrado")
    write_camada_parquet(brasil_integrado, bucket_name, "silver", "brasil_integrado")
    write_camada_parquet(alunos_limpo, bucket_name, "silver", "alunos_limpo")

    return {"status": "ok", "linhas_municipio_integrado": len(municipio_integrado)}, 200
```

- [ ] **Passo 4: Rodar e confirmar que passa**

```bash
pytest tests/test_process_silver.py -v
```
Expected: `2 passed`.

- [ ] **Passo 5: Commit**

```bash
git add pipeline/process_silver/main.py tests/test_process_silver.py
git commit -m "feat: Cloud Function process_silver integrando as bases com portão de qualidade"
```

---

### Task 16: Deploy de `process-silver` + Cloud Scheduler

- [ ] **Passo 1: Copiar arquivos compartilhados e ajustar imports**

```bash
cp pipeline/common/gcs_utils.py pipeline/process_silver/gcs_utils.py
```
Em `pipeline/process_silver/main.py`, troque os imports de
`pipeline.common.gcs_utils`/`pipeline.process_silver.quality`/
`pipeline.process_silver.transform` para imports locais (sem prefixo `pipeline.`):
```python
from gcs_utils import read_camada_latest, write_camada_parquet
from quality import validar_dataframe
from transform import (
    enriquecer_com_geografia,
    integrar_resultado_com_meta,
    padronizar_id_municipio,
    preencher_valores_ausentes_numericos,
    remover_linhas_duplicadas,
)
```

- [ ] **Passo 2: Criar `pipeline/process_silver/requirements.txt`**

```
pandas
great_expectations==1.19.0
google-cloud-storage
gcsfs
db-dtypes
functions-framework
```

- [ ] **Passo 3: Deploy**

```bash
gcloud functions deploy process-silver \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=pipeline/process_silver \
  --entry-point=process_silver \
  --trigger-http \
  --allow-unauthenticated \
  --memory=1Gi \
  --timeout=300s \
  --set-env-vars=BUCKET_NAME=SEU_PROJECT_ID-pipeline-alfabetizacao
```
(Memória maior aqui porque o Great Expectations consome mais RAM que as outras
funções.)

- [ ] **Passo 4: Criar o Cloud Scheduler job**

```bash
FUNCTION_URL=$(gcloud functions describe process-silver --gen2 --region=us-central1 --format='value(serviceConfig.uri)')
gcloud scheduler jobs create http job-process-silver \
  --location=us-central1 \
  --schedule="10 3 * * *" \
  --uri="$FUNCTION_URL" \
  --http-method=GET
```

- [ ] **Passo 5: Testar manualmente e verificar**

```bash
gcloud scheduler jobs run job-process-silver --location=us-central1
sleep 15
gcloud storage ls gs://SEU_PROJECT_ID-pipeline-alfabetizacao/silver/
```
Expected: pastas `municipio_integrado/`, `uf_integrado/`, `brasil_integrado/`,
`alunos_limpo/`, cada uma com `latest/`.

- [ ] **Passo 6: Commit, push e PR**

```bash
git add pipeline/process_silver
git commit -m "feat: deploy da Cloud Function process-silver com agendamento"
git push -u origin feature/silver-quality
gh pr create --title "feat: camada Silver (limpeza, qualidade, integração)" --body "Implementa e implanta process-silver: limpeza, validação Great Expectations e integração das 6 fontes."
```
Revise e faça merge, depois:
```bash
git checkout main && git pull origin main
```

---

## Fase 5 — Gold: camada analítica

**Branch:** `feature/gold-layer`

```bash
git checkout -b feature/gold-layer
```

### Task 17: Escrever e testar `pipeline/process_gold/aggregate.py`

**O que:** três funções: comparar resultado com meta (calcula o "gap" e se atingiu),
juntar os 3 níveis geográficos (município/UF/Brasil) num único dataset de
comparação, e calcular a evolução temporal do indicador.

**Por quê:** são exatamente os 3 exemplos de dataset Gold citados no PDF do desafio:
"indicador por município", "comparação entre metas e resultados", "evolução
temporal".

- [ ] **Passo 1: Escrever os testes primeiro**

Crie `tests/test_gold_aggregate.py`:
```python
import pandas as pd

from pipeline.process_gold.aggregate import (
    calcular_evolucao_temporal,
    comparar_meta_vs_resultado,
    montar_comparacao_multi_nivel,
)


def test_comparar_meta_vs_resultado_calcula_gap_e_flag():
    df = pd.DataFrame({"taxa_alfabetizacao": [85.0, 95.0], "meta_alfabetizacao_2024": [90.0, 90.0]})

    resultado = comparar_meta_vs_resultado(df)

    assert resultado["gap_meta_resultado"].tolist() == [-5.0, 5.0]
    assert resultado["atingiu_meta"].tolist() == [False, True]


def test_calcular_evolucao_temporal_agrupa_por_ano_e_localidade():
    df = pd.DataFrame({
        "sigla_uf": ["SP", "SP", "RJ"],
        "ano": [2023, 2024, 2023],
        "taxa_alfabetizacao": [80.0, 85.0, 70.0],
    })

    resultado = calcular_evolucao_temporal(df, colunas_grupo=["sigla_uf"], coluna_indicador="taxa_alfabetizacao")

    assert len(resultado) == 3
    linha_sp_2024 = resultado[(resultado["sigla_uf"] == "SP") & (resultado["ano"] == 2024)]
    assert linha_sp_2024["taxa_alfabetizacao"].iloc[0] == 85.0


def test_montar_comparacao_multi_nivel_junta_os_tres_niveis():
    municipio = pd.DataFrame({
        "nome": ["São Paulo"], "ano": [2024], "rede": ["municipal"],
        "taxa_alfabetizacao": [85.0], "meta_alfabetizacao_2024": [90.0],
        "gap_meta_resultado": [-5.0], "atingiu_meta": [False],
    })
    uf = pd.DataFrame({
        "sigla_uf": ["SP"], "ano": [2024], "rede": ["municipal"],
        "taxa_alfabetizacao": [83.0], "meta_alfabetizacao_2024": [88.0],
        "gap_meta_resultado": [-5.0], "atingiu_meta": [False],
    })
    brasil = pd.DataFrame({
        "ano": [2024], "rede": ["municipal"],
        "taxa_alfabetizacao": [80.0], "meta_alfabetizacao_2024": [82.0],
        "gap_meta_resultado": [-2.0], "atingiu_meta": [False],
    })

    resultado = montar_comparacao_multi_nivel(municipio, uf, brasil)

    assert set(resultado["nivel_geografico"]) == {"municipio", "uf", "brasil"}
    assert len(resultado) == 3
```

- [ ] **Passo 2: Rodar e confirmar que falha**

```bash
pytest tests/test_gold_aggregate.py -v
```
Expected: `ModuleNotFoundError`.

- [ ] **Passo 3: Implementar**

Crie `pipeline/process_gold/__init__.py` (vazio) e
`pipeline/process_gold/aggregate.py`:
```python
import pandas as pd


def comparar_meta_vs_resultado(
    df: pd.DataFrame,
    coluna_resultado: str = "taxa_alfabetizacao",
    coluna_meta: str = "meta_alfabetizacao_2024",
) -> pd.DataFrame:
    df = df.copy()
    df["gap_meta_resultado"] = df[coluna_resultado] - df[coluna_meta]
    df["atingiu_meta"] = df["gap_meta_resultado"] >= 0
    return df


def calcular_evolucao_temporal(
    df: pd.DataFrame,
    colunas_grupo: list,
    coluna_indicador: str = "taxa_alfabetizacao",
) -> pd.DataFrame:
    return (
        df.groupby(colunas_grupo + ["ano"], as_index=False)[coluna_indicador]
        .mean()
        .sort_values(colunas_grupo + ["ano"])
        .reset_index(drop=True)
    )


def montar_comparacao_multi_nivel(
    df_municipio: pd.DataFrame,
    df_uf: pd.DataFrame,
    df_brasil: pd.DataFrame,
) -> pd.DataFrame:
    colunas_comuns = [
        "nivel_geografico", "localidade", "ano", "rede",
        "taxa_alfabetizacao", "meta_alfabetizacao_2024",
        "gap_meta_resultado", "atingiu_meta",
    ]

    municipio = df_municipio.assign(nivel_geografico="municipio", localidade=df_municipio["nome"])
    uf = df_uf.assign(nivel_geografico="uf", localidade=df_uf["sigla_uf"])
    brasil = df_brasil.assign(nivel_geografico="brasil", localidade="Brasil")

    return pd.concat(
        [municipio[colunas_comuns], uf[colunas_comuns], brasil[colunas_comuns]],
        ignore_index=True,
    )
```

- [ ] **Passo 4: Rodar e confirmar que passa**

```bash
pytest tests/test_gold_aggregate.py -v
```
Expected: `3 passed`.

- [ ] **Passo 5: Commit**

```bash
git add pipeline/process_gold/__init__.py pipeline/process_gold/aggregate.py tests/test_gold_aggregate.py
git commit -m "feat: funções de agregação da camada Gold"
```

---

### Task 18: Escrever `pipeline/process_gold/main.py`

**O que:** lê os 3 datasets do silver (`municipio_integrado`, `uf_integrado`,
`brasil_integrado`), aplica as funções de agregação, e carrega os 3 resultados no
BigQuery: `indicador_por_municipio`, `comparacao_meta_resultado`,
`evolucao_temporal`.

- [ ] **Passo 1: Escrever o teste primeiro**

Crie `tests/test_process_gold.py`:
```python
import pandas as pd

from pipeline.process_gold import main as process_gold


def _silver_fake(nome_tabela):
    dados = {
        "municipio_integrado": pd.DataFrame({
            "nome": ["São Paulo"], "sigla_uf": ["SP"], "ano": [2024], "rede": ["municipal"],
            "taxa_alfabetizacao": [85.0], "meta_alfabetizacao_2024": [90.0],
        }),
        "uf_integrado": pd.DataFrame({
            "sigla_uf": ["SP"], "ano": [2024], "rede": ["municipal"],
            "taxa_alfabetizacao": [83.0], "meta_alfabetizacao_2024": [88.0],
        }),
        "brasil_integrado": pd.DataFrame({
            "ano": [2024], "rede": ["municipal"],
            "taxa_alfabetizacao": [80.0], "meta_alfabetizacao_2024": [82.0],
        }),
    }
    return dados[nome_tabela]


def test_process_gold_carrega_as_tres_tabelas_no_bigquery(monkeypatch):
    tabelas_carregadas = []

    monkeypatch.setattr(process_gold, "read_camada_latest", lambda bucket, camada, tabela: _silver_fake(tabela))
    monkeypatch.setattr(
        process_gold,
        "load_dataframe_to_bq",
        lambda df, project_id, dataset_id, table_id: tabelas_carregadas.append(table_id),
    )
    monkeypatch.setenv("BUCKET_NAME", "meu-bucket")
    monkeypatch.setenv("PROJECT_ID", "meu-projeto")

    resultado, status = process_gold.process_gold(request=None)

    assert status == 200
    assert set(tabelas_carregadas) == {
        "indicador_por_municipio", "comparacao_meta_resultado", "evolucao_temporal",
    }
```

- [ ] **Passo 2: Rodar e confirmar que falha**

```bash
pytest tests/test_process_gold.py -v
```
Expected: `ModuleNotFoundError`.

- [ ] **Passo 3: Implementar**

Crie `pipeline/process_gold/main.py`:
```python
import os

import functions_framework

from pipeline.common.bq_utils import load_dataframe_to_bq
from pipeline.common.gcs_utils import read_camada_latest
from pipeline.process_gold.aggregate import (
    calcular_evolucao_temporal,
    comparar_meta_vs_resultado,
    montar_comparacao_multi_nivel,
)


@functions_framework.http
def process_gold(request):
    bucket_name = os.environ["BUCKET_NAME"]
    project_id = os.environ["PROJECT_ID"]

    municipio = read_camada_latest(bucket_name, "silver", "municipio_integrado")
    uf = read_camada_latest(bucket_name, "silver", "uf_integrado")
    brasil = read_camada_latest(bucket_name, "silver", "brasil_integrado")

    municipio = comparar_meta_vs_resultado(municipio)
    uf = comparar_meta_vs_resultado(uf)
    brasil = comparar_meta_vs_resultado(brasil)

    comparacao = montar_comparacao_multi_nivel(municipio, uf, brasil)
    evolucao = calcular_evolucao_temporal(comparacao, colunas_grupo=["nivel_geografico", "localidade"])

    load_dataframe_to_bq(municipio, project_id, "gold_alfabetizacao", "indicador_por_municipio")
    load_dataframe_to_bq(comparacao, project_id, "gold_alfabetizacao", "comparacao_meta_resultado")
    load_dataframe_to_bq(evolucao, project_id, "gold_alfabetizacao", "evolucao_temporal")

    return {"status": "ok", "linhas_indicador_por_municipio": len(municipio)}, 200
```

- [ ] **Passo 4: Rodar e confirmar que passa**

```bash
pytest tests/test_process_gold.py -v
```
Expected: `1 passed`.

- [ ] **Passo 5: Commit**

```bash
git add pipeline/process_gold/main.py tests/test_process_gold.py
git commit -m "feat: Cloud Function process_gold com as 3 tabelas analíticas"
```

---

### Task 19: Deploy de `process-gold` + Cloud Scheduler

- [ ] **Passo 1: Copiar arquivos compartilhados e ajustar imports**

```bash
cp pipeline/common/gcs_utils.py pipeline/process_gold/gcs_utils.py
cp pipeline/common/bq_utils.py pipeline/process_gold/bq_utils.py
```
Em `pipeline/process_gold/main.py`, troque os imports:
```python
from bq_utils import load_dataframe_to_bq
from gcs_utils import read_camada_latest
from aggregate import (
    calcular_evolucao_temporal,
    comparar_meta_vs_resultado,
    montar_comparacao_multi_nivel,
)
```

- [ ] **Passo 2: Criar `pipeline/process_gold/requirements.txt`**

```
pandas
google-cloud-bigquery
google-cloud-storage
gcsfs
db-dtypes
functions-framework
```

- [ ] **Passo 3: Deploy**

```bash
gcloud functions deploy process-gold \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=pipeline/process_gold \
  --entry-point=process_gold \
  --trigger-http \
  --allow-unauthenticated \
  --memory=512Mi \
  --timeout=300s \
  --set-env-vars=BUCKET_NAME=SEU_PROJECT_ID-pipeline-alfabetizacao,PROJECT_ID=SEU_PROJECT_ID
```

- [ ] **Passo 4: Criar o Cloud Scheduler job**

```bash
FUNCTION_URL=$(gcloud functions describe process-gold --gen2 --region=us-central1 --format='value(serviceConfig.uri)')
gcloud scheduler jobs create http job-process-gold \
  --location=us-central1 \
  --schedule="20 3 * * *" \
  --uri="$FUNCTION_URL" \
  --http-method=GET
```

- [ ] **Passo 5: Testar manualmente e verificar no BigQuery**

```bash
gcloud scheduler jobs run job-process-gold --location=us-central1
sleep 15
bq query --use_legacy_sql=false 'SELECT * FROM `SEU_PROJECT_ID.gold_alfabetizacao.indicador_por_municipio` LIMIT 10'
```
Expected: linhas retornadas com `nome`, `sigla_uf`, `taxa_alfabetizacao`,
`gap_meta_resultado`, `atingiu_meta`.

- [ ] **Passo 6: Commit, push e PR**

```bash
git add pipeline/process_gold
git commit -m "feat: deploy da Cloud Function process-gold com agendamento"
git push -u origin feature/gold-layer
gh pr create --title "feat: camada Gold (agregações analíticas no BigQuery)" --body "Implementa e implanta process-gold: comparação meta/resultado, multi-nível geográfico e evolução temporal."
```
Revise e faça merge, depois:
```bash
git checkout main && git pull origin main
```

---

## Fase 6 — Monitoramento

**Branch:** `feature/monitoring`

```bash
git checkout -b feature/monitoring
```

### Task 20: Criar canal de notificação por e-mail e política de alerta

**O que:** configurar o Cloud Monitoring para te avisar por e-mail se qualquer uma
das 4 Cloud Functions falhar durante a execução.

**Por quê:** é o item opcional de monitoramento do desafio — falhas de ingestão e
alertas de erro.

- [ ] **Passo 1: Criar o canal de notificação (e-mail)**

```bash
gcloud alpha monitoring channels create \
  --display-name="Alertas Pipeline Alfabetização" \
  --type=email \
  --channel-labels=email_address=SEU_EMAIL
```
Anote o `name` retornado (algo como
`projects/SEU_PROJECT_ID/notificationChannels/1234567890`) — você vai precisar dele
no próximo passo.

- [ ] **Passo 2: Criar a política de alerta para falhas de execução**

Crie `monitoring/alert-policy-falhas.json`:
```json
{
  "displayName": "Falha em Cloud Function da pipeline de alfabetização",
  "combiner": "OR",
  "conditions": [
    {
      "displayName": "Execuções com erro (log severity ERROR)",
      "conditionMatchedLog": {
        "filter": "resource.type=\"cloud_run_revision\" AND severity>=ERROR AND resource.labels.service_name=(\"ingest-batch\" OR \"ingest-stream\" OR \"process-silver\" OR \"process-gold\")"
      }
    }
  ],
  "alertStrategy": {
    "notificationRateLimit": {
      "period": "300s"
    }
  }
}
```

```bash
mkdir -p monitoring
gcloud alpha monitoring policies create --policy-from-file=monitoring/alert-policy-falhas.json --notification-channels=NOME_DO_CANAL_DO_PASSO_1
```

- [ ] **Passo 3: Verificar que a política foi criada**

```bash
gcloud alpha monitoring policies list --format="table(displayName,enabled)"
```
Expected: `Falha em Cloud Function da pipeline de alfabetização` com `enabled=True`.

---

### Task 21: Testar o alerta forçando uma falha

**O que:** provocar deliberadamente um erro numa função e confirmar que o e-mail
chega.

- [ ] **Passo 1: Rodar a função com uma variável de ambiente faltando (vai falhar de propósito)**

```bash
gcloud functions deploy ingest-batch \
  --gen2 \
  --region=us-central1 \
  --update-env-vars=BUCKET_NAME=bucket-que-nao-existe-123456
gcloud scheduler jobs run job-ingest-batch --location=us-central1
```

- [ ] **Passo 2: Verificar o log de erro**

```bash
gcloud functions logs read ingest-batch --gen2 --region=us-central1 --limit=10
```
Expected: uma linha com erro relacionado ao bucket inexistente.

- [ ] **Passo 3: Confirmar o e-mail de alerta (pode levar alguns minutos)**

Verifique sua caixa de entrada. Se não chegar em ~10 minutos, confira em
"Cloud Monitoring" → "Alerting" → "Incidents" no console se o incidente foi aberto.

- [ ] **Passo 4: Reverter a variável de ambiente para o valor correto**

```bash
gcloud functions deploy ingest-batch \
  --gen2 \
  --region=us-central1 \
  --update-env-vars=BUCKET_NAME=SEU_PROJECT_ID-pipeline-alfabetizacao
```

- [ ] **Passo 5: Commit e PR**

```bash
git add monitoring
git commit -m "feat: alerta de monitoramento para falhas nas Cloud Functions"
git push -u origin feature/monitoring
gh pr create --title "feat: monitoramento e alertas (Cloud Monitoring)" --body "Adiciona canal de notificação por e-mail e política de alerta para falhas de execução das Cloud Functions."
```
Revise e faça merge, depois:
```bash
git checkout main && git pull origin main
```

---

## Fase 7 — FinOps e README

**Branch:** `docs/readme-and-finops`

```bash
git checkout -b docs/readme-and-finops
```

### Task 22: Conferir o custo real no Billing

**O que:** olhar o relatório de custos do GCP para confirmar que a pipeline ficou
dentro do free tier, e anotar os números reais para colocar no README.

- [ ] **Passo 1: Acessar o relatório**

Vá em https://console.cloud.google.com/billing/reports, filtre pelo seu projeto e
pelos últimos 7 dias.

- [ ] **Passo 2: Anotar os valores por serviço**

Anote o custo de: Cloud Functions, Cloud Storage, BigQuery, Pub/Sub, Cloud Scheduler,
Cloud Monitoring. Esperado: R$ 0,00 ou muito próximo disso (dentro do free tier).

- [ ] **Passo 3: Guardar essa informação** para usar na Task 23 (seção FinOps do README).

---

### Task 23: Escrever o README completo

**O que:** o README final, cobrindo todos os pontos exigidos pelo desafio: contexto
do problema, arquitetura, diagrama, fluxo de dados, tecnologias e justificativas,
decisões arquiteturais (trade-offs), monitoramento e FinOps, e aplicação em IA.

**Por quê:** é a entrega mais pesada em nota depois do código em si — o desafio pede
explicitamente que "vá além de uma explicação técnica básica".

- [ ] **Passo 1: Criar o `README.md`** na raiz do repositório, cobrindo (você pode
copiar a estrutura já validada do spec em
`docs/superpowers/specs/2026-07-13-pipeline-alfabetizacao-design.md` como ponto de
partida, e enriquecer com o que foi de fato implementado):

1. Contexto do problema (Compromisso Nacional Criança Alfabetizada, Indicador,
   ponto de corte 743 no Saeb) — pode reaproveitar o texto do PDF do desafio.
2. Explicação do desafio educacional e do uso do indicador.
3. Arquitetura proposta (use o diagrama ASCII do spec como base, ou gere um
   diagrama visual).
4. Fluxo de dados (bronze → silver → gold, batch e streaming).
5. Tecnologias utilizadas e por quê cada uma foi escolhida (GCP, Pandas, Great
   Expectations, Pub/Sub, Cloud Scheduler — mencione que a escolha seguiu o que foi
   ensinado na disciplina Data Prepare).
6. Decisões arquiteturais / trade-offs: batch vs. streaming, Pandas vs. Spark,
   `--allow-unauthenticated` por simplicidade, `dt=`/`latest` para histórico +
   praticidade.
7. Monitoramento e FinOps: os números reais da Task 22, as escolhas de
   particionamento/seleção de colunas, e a política de alerta configurada.
8. Aplicação em IA: como a tabela `indicador_por_municipio` do Gold poderia
   alimentar um modelo de predição de alfabetização (features disponíveis: taxa
   histórica, gap para a meta, região), análise de desigualdade educacional
   (comparação entre UFs/municípios) e políticas públicas baseadas em dados.
9. Estrutura do repositório (pode copiar do spec).
10. Como rodar localmente (resumo dos comandos das Tasks 1-4).

- [ ] **Passo 2: Revisar o README lendo do zero**, como se você não soubesse nada do
projeto — confirme que dá pra entender o "porquê" de cada decisão sem precisar
perguntar a ninguém.

- [ ] **Passo 3: Commit, push e PR final**

```bash
git add README.md
git commit -m "docs: README completo com arquitetura, decisões, FinOps e aplicação em IA"
git push -u origin docs/readme-and-finops
gh pr create --title "docs: README completo do projeto" --body "README final cobrindo todos os pontos exigidos pelo desafio: arquitetura, decisões, FinOps, monitoramento e aplicação em IA."
```
Revise e faça merge para `main`.

---

## Fase 8 — Vídeo executivo (não é código)

### Task 24: Preparar o roteiro do vídeo (até 5 minutos)

**O que:** um roteiro curto para gravar, cobrindo problema de negócio, arquitetura
da solução, valor da pipeline, e potencial de IA — em linguagem executiva (não
técnica).

- [ ] **Passo 1: Escrever um roteiro de ~500 palavras** (cabe em ~5 min falado)
cobrindo, nessa ordem: (1) o problema — crianças não alfabetizadas até o 2º ano
prejudicam toda a trajetória educacional, e faltam dados integrados para agir; (2) a
solução — pipeline automatizada que integra 6 fontes públicas todo dia, valida
qualidade, e disponibiliza indicadores prontos; (3) o valor — decisões de política
pública mais rápidas e baseadas em evidência, com custo de infraestrutura
praticamente zero; (4) potencial de IA — a mesma base Gold já está pronta para
alimentar modelos preditivos e dashboards.

- [ ] **Passo 2: Gravar o vídeo** (até 5 minutos, pelo menos um integrante
aparecendo, linguagem de apresentação para liderança/stakeholders — não é uma aula
técnica).

- [ ] **Passo 3: Anotar o link do vídeo no `logbook.md`** e, se fizer sentido,
referenciar no README.

---

## Self-Review deste plano

- **Cobertura do spec:** todos os itens obrigatórios (batch, streaming, bronze,
  silver, gold, qualidade, cloud, FinOps, estrutura do repo, Git com PRs, README,
  vídeo) e o opcional escolhido (monitoramento) têm tarefa correspondente. O
  enriquecimento externo e o modelo de ML seguem fora de escopo, como decidido.
- **Consistência de nomes:** `write_camada_parquet`/`read_camada_latest` são usados
  com a mesma assinatura em todas as camadas; `gcs_utils.py`/`bq_utils.py` mantêm o
  mesmo nome de função entre a versão em `pipeline/common/` e as cópias locais de
  cada function.
- **Sem placeholders:** todo bloco de código é completo e roda de verdade — os
  únicos valores que você precisa substituir são `SEU_PROJECT_ID` e `SEU_EMAIL`,
  explicitamente marcados desde as Global Constraints.
