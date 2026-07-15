# Pipeline Híbrido para Análise da Alfabetização no Brasil

Tech Challenge — Fase 2 (FIAP). Pipeline de dados híbrida (batch + streaming),
implementada de ponta a ponta no Google Cloud Platform, que integra dados públicos
do Indicador Criança Alfabetizada para apoiar análises sobre alfabetização infantil
no Brasil.

## Vídeo executivo

[Assista à apresentação executiva do projeto](https://drive.google.com/file/d/19tn9dVYyO0q19eisqMMk_5FLHEn-v0Yw/view?usp=sharing)

## Contexto do problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento
educacional, social e econômico de um país. O **Compromisso Nacional Criança
Alfabetizada** é uma política pública que mobiliza União, estados, Distrito Federal
e municípios com o objetivo de garantir que todas as crianças brasileiras estejam
alfabetizadas até o final do 2º ano do ensino fundamental.

Em 2023, o INEP realizou a **Pesquisa Alfabetiza Brasil**, que definiu o ponto de
corte de **743 pontos** na escala de proficiência do Saeb como o nível a partir do
qual uma criança é considerada alfabetizada. A partir desse parâmetro foi criado o
**Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que
atingem esse patamar — com meta nacional de que, até 2030, todas as crianças
brasileiras estejam alfabetizadas ao final do 2º ano.

Entender os fatores que influenciam a alfabetização exige integrar múltiplas fontes
de dados — metas nacionais, estaduais e municipais, dados territoriais e
microdados educacionais — em vez de olhar indicadores isoladamente. É esse
problema de integração, qualidade e disponibilização de dados que esta pipeline
resolve.

## O desafio técnico

Construir, como um time de engenharia de dados de uma organização pública de
análise educacional, uma pipeline híbrida (batch + streaming) que:

- integra as 6 fontes de dados exigidas (UF, Meta Alfabetização Brasil/UF/Município,
  Município, Dados de Alunos);
- segue a Arquitetura Medalhão (Bronze/Silver/Gold);
- garante qualidade de dados (duplicidade, nulos, chaves, consistência);
- roda em nuvem com custo controlado (FinOps);
- é monitorada (observabilidade de falhas).

## Arquitetura

```
Base dos Dados (BigQuery público "basedosdados")
        │
        ├── BATCH (Cloud Scheduler, 03:00 UTC diário)
        │         ──► Cloud Function "ingest-batch"
        │             (8 tabelas: as 6 exigidas + dicionário + geografia)
        │
        └── STREAMING ──► script publisher ──► Pub/Sub "alfabetizacao-eventos"
                          (simula eventos: atualização de indicador)
                                    │
                                    ▼ (trigger automático)
                          Cloud Function "ingest-stream"
                                    │
                                    ▼
                    ┌───────────────────────────────────┐
                    │   BRONZE (GCS, Parquet)            │
                    │   dt=AAAA-MM-DD/ (histórico)        │
                    │   latest/ (sempre atualizado)       │
                    └───────────────────────────────────┘
                                    │
                    Cloud Scheduler (03:10 UTC) ──► Cloud Function "process-silver"
                    (Pandas: limpeza, decodificação de "rede", padronização de
                    chaves, integração das bases + Great Expectations: qualidade)
                                    │
                                    ▼
                    ┌───────────────────────────────────┐
                    │   SILVER (GCS, Parquet)             │
                    │   municipio_integrado, uf_integrado, │
                    │   brasil_integrado, alunos_limpo     │
                    └───────────────────────────────────┘
                                    │
                    Cloud Scheduler (03:20 UTC) ──► Cloud Function "process-gold"
                    (Pandas: gap vs. meta, comparação multi-nível, evolução temporal)
                                    │
                                    ▼
                    ┌───────────────────────────────────┐
                    │   GOLD (BigQuery, dataset           │
                    │   gold_alfabetizacao)                │
                    │   indicador_por_municipio,            │
                    │   comparacao_meta_resultado,          │
                    │   evolucao_temporal                   │
                    └───────────────────────────────────┘
                                    │
                                    ▼
                    Cloud Logging + Cloud Monitoring
                    (alerta por e-mail em falha de qualquer function)
```

### Fluxo de dados

1. **Ingestão batch** (`ingest-batch`): todo dia às 3h, consulta as 8 tabelas fonte
   no BigQuery público da Base dos Dados (`basedosdados.br_inep_avaliacao_alfabetizacao`
   e a tabela de geografia `basedosdados.br_bd_diretorios_brasil.municipio`) e grava
   cada uma como Parquet cru na camada Bronze.
2. **Ingestão streaming** (`publish_stream_events.py` + `ingest-stream`): um script
   publica eventos simulados (ex.: "indicador de SP atualizado") no Pub/Sub; a Cloud
   Function consome cada evento automaticamente e grava no Bronze.
3. **Processamento Silver** (`process-silver`): lê o Bronze, remove duplicatas,
   preenche valores ausentes, decodifica códigos categóricos (ver "Decisões
   arquiteturais"), integra resultado real com meta e com geografia, valida
   qualidade com Great Expectations, e só grava no Silver se a validação passar.
4. **Processamento Gold** (`process-gold`): lê o Silver, calcula o gap entre
   resultado e meta, monta a comparação entre os 3 níveis geográficos, calcula a
   evolução temporal, e carrega tudo no BigQuery.
5. **Monitoramento**: Cloud Logging registra cada execução; uma política do Cloud
   Monitoring dispara um e-mail de alerta se qualquer uma das 4 Cloud Functions
   falhar.

## Tecnologias utilizadas e por quê

| Tecnologia | Uso | Justificativa |
|---|---|---|
| **GCP** | Cloud provider | Os dados fonte (Base dos Dados) já são publicados como dataset público no BigQuery — usar GCP elimina uma etapa de extração para outra nuvem, e o free tier é generoso o suficiente para todo este projeto. |
| **Pandas + Great Expectations** | Processamento e qualidade de dados | Replica o pipeline hands-on ensinado na disciplina de Data Prepare do curso (Bronze/Silver/Gold com Pandas + Great Expectations). O volume de dados do desafio (dezenas de milhares de linhas na maioria das tabelas, ~3,9M na maior) não justifica Spark. |
| **Cloud Functions (Gen2)** | Compute serverless | Sem servidor para gerenciar, cobrança só pelo tempo de execução, integra nativamente com Cloud Scheduler e Pub/Sub. |
| **Cloud Scheduler** | Orquestração do batch | Três jobs com horários escalonados (03:00/03:10/03:20) — simples de entender e depurar, sem precisar de um orquestrador dedicado (Airflow) para o tamanho deste projeto. |
| **Pub/Sub** | Streaming | Equivalente gerenciado e serverless do Kafka ensinado em aula — mesmo padrão conceitual (producer/consumer/tópico), sem precisar manter um broker no ar. |
| **GCS (Parquet)** | Armazenamento Bronze/Silver | Parquet é colunar e comprimido — mais barato e rápido de processar que CSV. Convenção `dt=`/`latest` preserva histórico completo (exigido na Bronze) e simplifica a leitura da camada seguinte. |
| **BigQuery** | Camada Gold | Já é onde os dados fonte vivem; manter o resultado final lá também simplifica consulta SQL/dashboards sem mover dados entre serviços. |
| **Cloud Monitoring/Logging** | Observabilidade | Nativos do GCP, sem custo adicional de configuração, cobrem o item opcional de monitoramento do desafio. |

## Decisões arquiteturais (trade-offs)

- **Batch vs. streaming:** o batch cobre a ingestão estrutural/histórica (as 8
  tabelas fonte, atualizadas 1x/dia); o streaming simula a chegada de eventos
  individuais (novas medições) via Pub/Sub — a diferença de trigger (agenda vs.
  evento) é o que de fato distingue as duas abordagens, não apenas a fonte de dados.
- **Pandas vs. Spark:** optamos por Pandas por replicar o que foi ensinado em aula e
  por ser suficiente para o volume real dos dados (a maior tabela, `alunos`, tem
  ~3,9 milhões de linhas — grande para uma planilha, pequeno para dados distribuídos
  de verdade). Uma migração para Spark/Dataproc seria justificável se o volume
  crescesse em ordens de grandeza.
- **Custo vs. simplicidade de deploy:** as Cloud Functions HTTP usam
  `--allow-unauthenticated` para o Cloud Scheduler poder chamá-las sem configurar
  autenticação OIDC — uma simplificação consciente e aceitável para um projeto de
  estudo com dados públicos, mas que não seria recomendada em produção com dados
  sensíveis.
- **Cada Cloud Function é uma pasta autocontida:** o Cloud Functions builda cada
  deploy isoladamente a partir da pasta apontada em `--source`. Em vez de um pacote
  Python compartilhado entre deploys (mais complexo de empacotar corretamente),
  escrevemos e testamos os utilitários uma vez em `pipeline/common/` e copiamos os
  arquivos prontos para dentro da pasta de cada função antes do deploy — uma
  pequena duplicação de arquivo em troca de deploys simples e previsíveis.
- **Memória/timeout ajustados na prática, não estimados de antemão:** os primeiros
  deploys de `ingest-batch` (512Mi/300s) estouraram por causa da tabela `alunos`
  (~3,9M linhas, que em memória como DataFrame passa de 512 MiB). Corrigido para
  2Gi/540s depois de medir o consumo real — decisão de dimensionamento baseada em
  dado observado, não em suposição.
- **Bug real de integração corrigido durante o desenvolvimento:** a coluna `rede`
  vem como código numérico ("3") nas tabelas de resultado real e como texto
  ("Municipal") nas tabelas de meta — um cruzamento direto entre elas falhava
  silenciosamente (toda meta ficava nula). Corrigido com uma função de
  decodificação, usando o mapeamento oficial da tabela `dicionario` do próprio
  dataset fonte. Detalhes completos no `logbook.md`.

## Regras de qualidade de dados

Implementadas com Great Expectations na camada Silver, sobre o dataset
`municipio_integrado` (o mais granular, citado como exemplo de Gold no desafio):

- **Duplicidade / chave de relacionamento:** `ExpectCompoundColumnsToBeUnique` sobre
  `(ano, id_municipio, rede)`.
- **Valores ausentes:** `ExpectColumnValuesToNotBeNull` em `id_municipio` e
  `taxa_alfabetizacao`.
- **Consistência entre tabelas:** `ExpectColumnValuesToBeBetween` garantindo que
  `taxa_alfabetizacao` esteja entre 0 e 100.

Se a validação falhar, a Cloud Function `process-silver` retorna erro (HTTP 500) e
**não grava nada no Silver** — a falha é capturada pelo Cloud Monitoring, que envia
um alerta por e-mail.

## Monitoramento e FinOps

### Monitoramento

Cloud Logging registra a execução de cada uma das 4 Cloud Functions. Uma política
do Cloud Monitoring (`monitoring/alert-policy-falhas.json`) dispara um alerta por
e-mail sempre que qualquer uma delas registra um log de severidade `ERROR`. Testado
na prática: uma falha real foi provocada (apontando a função para um bucket
inexistente), o erro apareceu nos logs em segundos, e o e-mail de alerta chegou com
sucesso.

### FinOps

Práticas aplicadas para manter o custo dentro do free tier:

- **Parquet + particionamento** no GCS (`camada/tabela/dt=data/` e `.../latest/`),
  em vez de CSV.
- **Seleção explícita de colunas** nas queries de ingestão (nunca `SELECT *`) — o
  BigQuery cobra por bytes de coluna escaneados; por exemplo, a query de `alunos`
  ignora colunas de logística de aplicação de prova (`caderno`,
  `preenchimento_caderno`) que não são usadas em nenhuma análise.
- **Memória dimensionada por medição real**, não superestimada por padrão — cada
  Cloud Function usa a menor memória que efetivamente roda sem erro.
- **`--allow-unauthenticated`** evita a complexidade (e possíveis custos indiretos
  de configuração) de autenticação OIDC entre Cloud Scheduler e Cloud Functions.

**Custo real medido** (via `INFORMATION_SCHEMA.JOBS_BY_PROJECT` do BigQuery e
`gcloud storage du`, em vez do relatório de Billing do console, que leva 24-48h
para consolidar):

| Serviço | Uso medido | Limite gratuito | % usado |
|---|---|---|---|
| BigQuery (consultas) | 255,5 MB processados/24h | 1 TB/mês | 0,024% |
| Cloud Storage | 287 MB no bucket | 5 GB/mês (us-central1) | 5,6% |
| Cloud Scheduler | 3 jobs | 3 jobs/conta | 100% (ainda gratuito) |
| Cloud Functions, Pub/Sub, Build, Monitoring | uso pontual de testes/deploys | limites generosos de cada serviço | desprezível |

**Custo total estimado: R$ 0,00** — a pipeline inteira roda dentro do free tier do
GCP.

## Aplicação em IA

A camada Gold (especialmente `indicador_por_municipio`, que já combina resultado
real, meta, geografia e o gap entre eles) está pronta para alimentar:

- **Modelos de predição de alfabetização:** usando como features a taxa histórica
  por município, a evolução temporal (`evolucao_temporal`), e o gap em relação à
  meta, um modelo de regressão/classificação poderia prever se um município tende a
  atingir a meta de 2030 antes mesmo do resultado ser medido — permitindo
  intervenção pública antecipada.
- **Análise de desigualdade educacional:** a tabela `comparacao_meta_resultado`
  (com os 3 níveis geográficos lado a lado) permite identificar municípios/UFs
  sistematicamente abaixo da média nacional, sinalizando desigualdade regional que
  merece atenção prioritária de política pública.
- **Clusters de vulnerabilidade educacional:** técnicas de clusterização (k-means,
  por exemplo) sobre `indicador_por_municipio` poderiam agrupar municípios com
  perfis semelhantes de desempenho/meta, orientando políticas públicas segmentadas
  por perfil em vez de uma abordagem única para todo o país.

Esta seção é intencionalmente apenas descritiva — nenhum modelo foi treinado, para
manter o foco no obrigatório do desafio (a pipeline em si) dentro do prazo
disponível.

## Estrutura do repositório

```
├── pipeline/
│   ├── common/            (gcs_utils.py, bq_utils.py — testados uma vez, copiados
│   │                        para dentro de cada função antes do deploy)
│   ├── ingest_batch/       (Cloud Function: batch, 8 tabelas fonte)
│   ├── ingest_stream/      (Cloud Function: streaming via Pub/Sub)
│   ├── process_silver/     (Cloud Function: limpeza, qualidade, integração)
│   └── process_gold/       (Cloud Function: agregações analíticas)
├── scripts/
│   └── publish_stream_events.py   (publisher de eventos simulados)
├── monitoring/
│   └── alert-policy-falhas.json
├── tests/                  (23 testes, TDD para toda a lógica de transformação)
├── logbook.md              (registro cronológico de cada decisão e passo tomado)
├── requirements.txt
└── README.md
```

## Git e desenvolvimento

O repositório usa uma branch por fase/funcionalidade (`chore/setup-gcp-and-repo`,
`feature/common-utils`, `feature/bronze-ingestion`, `feature/streaming`,
`feature/silver-quality`, `feature/gold-layer`, `feature/monitoring`,
`docs/readme-and-finops`), cada uma com commits pequenos e descritivos e uma Pull
Request própria (com descrição do que mudou e por quê) antes do merge em `main`.
Branches são mantidas após o merge (não deletadas), preservando o histórico
completo de evolução do projeto.

## Como rodar localmente

```bash
# 1. Clonar o repositório e criar o ambiente virtual
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Autenticar no GCP
gcloud auth login
gcloud auth application-default login
gcloud config set project SEU_PROJECT_ID

# 3. Rodar os testes
pytest -v
```

O histórico completo de cada decisão e descoberta feita durante o desenvolvimento
está registrado em `logbook.md`.
