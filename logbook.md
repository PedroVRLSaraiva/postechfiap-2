# Logbook — Tech Challenge Fase 2 (Pipeline Híbrido de Alfabetização)

Registro cronológico de todas as decisões e passos tomados no desenvolvimento do projeto.

## 2026-07-13

- Lido o documento `docs/[IAST] - Tech Challenge - Fase 2.pdf` na íntegra.
- Estruturados os requisitos do desafio: pipeline híbrida (batch + streaming), Arquitetura
  Medalhão (Bronze/Silver/Gold), integração de dados do Indicador Criança Alfabetizada
  (Base dos Dados/INEP), qualidade de dados, implementação em nuvem (AWS/GCP/Azure),
  FinOps, uso adequado de Git (branches/PRs), README completo e vídeo executivo.
- Iniciado processo de brainstorming para levantar decisões-chave (cloud provider, stack
  técnica, escopo de streaming, dados externos opcionais, etc.) antes de montar o plano
  de execução.
- Decisão: provedor de nuvem = **GCP**. Os dados fonte já estão no BigQuery ("Base dos
  Dados"), o que evita uma etapa de extração inicial para outra nuvem, e o GCP tem free
  tier generoso (BigQuery sandbox, GCS, Pub/Sub, Cloud Functions).
- Decisão: modo de execução = **GCP real, dentro do free tier** (não emulação local).
  A pipeline será implantada de fato no GCP, dimensionada para não sair do free tier,
  gerando evidências reais de custo/FinOps para o README e para o vídeo executivo.
- Pesquisado `docs/FIAP_Fase2_DataPrepare_Transcricoes.txt` (transcrições da disciplina
  Data Prepare) para alinhar as decisões técnicas com o que foi efetivamente ensinado em
  aula. Achado: o laboratório principal do curso foi Bronze/Silver/Gold com
  **Pandas + Great Expectations** (Colab), com PySpark ensinado como caminho de escala;
  o único laboratório de nuvem hands-on foi **AWS Glue + S3 + Step Functions**; streaming
  foi ensinado a fundo com **Kafka** (producer/consumer, janelas, agregações); GCP
  (BigQuery/Dataflow/Dataproc) apareceu só em teoria, nunca codado em aula.
- Decisão: engine de processamento das camadas = **Pandas + Great Expectations**,
  rodando em Cloud Functions/Cloud Run, gravando Parquet no GCS (bronze/silver) e
  carregando o Gold no BigQuery para consulta SQL/dashboards. Motivo: replica o que foi
  ensinado em aula, é a opção mais simples operacionalmente (sem cluster para gerenciar)
  e o volume de dados do desafio (indicadores por UF/município) não justifica Spark.
- Decisão: ingestão streaming = **Pub/Sub**. Um script Python publica eventos simulados
  (atualização de indicadores/metas) num tópico; uma Cloud Function consome e processa.
  Motivo: serverless, sem infraestrutura para gerenciar, dentro do free tier, e mantém o
  mesmo padrão conceitual producer/consumer ensinado com Kafka em aula.
- Decisão: **sem enriquecimento com dados externos** por agora (opcional do desafio).
  Foco total nas fontes obrigatórias do indicador de alfabetização; pode ser adicionado
  depois se sobrar tempo.
- Decisão: **monitoramento básico incluído** (opcional do desafio), usando Cloud Logging
  + Cloud Monitoring nativos do GCP para métricas de execução, erros e um alerta simples
  por e-mail em caso de falha. Baixo esforço já que vem de graça com Cloud
  Functions/Pub-Sub.
- Decisão: seção "Aplicação em IA" do README será **apenas documentada** (sem treinar
  modelo de fato). O desafio pede para explicar o potencial de uso da camada Gold, não
  exige o modelo em si — mantém o foco no que é obrigatório: a pipeline.
- Prazo real informado: **25 horas** para entregar tudo (não "algumas horas" como
  inicialmente dito). Isso reabriu a discussão de escopo: com 25h há folga suficiente
  para a implementação serverless completa, então os cortes de escopo por medo de tempo
  foram descartados.
- Decisão final de escopo = **Cenário C**: implementação serverless completa e
  automatizada no GCP —
  - Batch: Cloud Scheduler → Cloud Function (ingestão periódica de metas/município/dados
    nacionais via query no BigQuery público da Base dos Dados);
  - Streaming: Pub/Sub → Cloud Function com trigger automático (simulação de eventos
    quase real: atualização de indicadores/metas);
  - Bronze/Silver/Gold processados com Pandas + Great Expectations (alinhado ao que foi
    ensinado em aula);
  - Silver grava Parquet particionado no GCS; Gold carregado no BigQuery;
  - Monitoramento real via Cloud Logging + Cloud Monitoring com alertas (item opcional
    do desafio, incluído);
  - Sem enriquecimento com dados externos (opcional descartado por ora);
  - Aplicação em IA apenas documentada no README, sem treinar modelo;
  - Repositório com branches por funcionalidade, commits descritivos e PRs (trabalho
    solo, mas mantendo o fluxo de Git exigido no desafio).
  - Motivo: cumpre 100% do obrigatório com automação real (distinguindo de fato batch de
    streaming), inclui o opcional de monitoramento, e ainda deixa ~13-17h de folga do
    prazo de 25h para debugar deploy, escrever o README e gravar o vídeo executivo.
- Design consolidado apresentado e aprovado pelo usuário. Requisito adicional definido
  para o spec e para o plano de execução: **manter tudo o mais simples possível**, e
  cada passo do plano deve ser executável por alguém sem experiência prévia, com
  explicação clara do que está sendo feito e por quê em cada etapa.
- Spec de design escrito em
  `docs/superpowers/specs/2026-07-13-pipeline-alfabetizacao-design.md`. Autorevisão
  identificou uma ambiguidade (o que aciona `process_silver`/`process_gold`) e foi
  corrigida: as três etapas de batch (`ingest_batch`, `process_silver`,
  `process_gold`) são acionadas por três jobs do Cloud Scheduler em horários
  escalonados, em vez de encadeamento por eventos — mais simples de entender e
  depurar por quem não tem experiência prévia.
- Repositório Git inicializado localmente (branch `main`), com `.gitignore` (excluindo
  credenciais GCP e o arquivo de transcrições de aula) e primeiro commit contendo o PDF
  do desafio, o spec de design e este logbook.
- Remote `origin` adicionado apontando para
  `https://github.com/PedroVRLSaraiva/postechfiap-2.git`.
- Push inicial feito: branch `main` publicada no GitHub (`origin/main`).
- Iniciada a skill `writing-plans` para transformar o spec em plano de execução.
- Investigado o schema real das tabelas fonte no BigQuery público (via console do
  usuário, projeto `basedosdados`), em vez de adivinhar nomes de coluna. Resultado:
  - Dataset `basedosdados.br_inep_avaliacao_alfabetizacao` contém as 6 tabelas exigidas
    pelo desafio: `alunos`, `meta_alfabetizacao_brasil`, `meta_alfabetizacao_municipio`,
    `meta_alfabetizacao_uf`, `municipio`, `uf` — mais uma tabela auxiliar `dicionario`
    (decodifica valores categóricos como `rede`).
  - Chaves de junção identificadas: `id_municipio` (STRING) liga `alunos` ↔ `municipio`
    ↔ `meta_alfabetizacao_municipio`; `sigla_uf` liga `uf` ↔ `meta_alfabetizacao_uf`;
    `meta_alfabetizacao_brasil` só tem `ano`+`rede` (é agregado nacional sem chave
    geográfica).
  - Nenhuma tabela do dataset tem nome de município/UF por extenso — confirmado que
    `basedosdados.br_bd_diretorios_brasil.municipio` (tabela de referência geográfica
    padrão da própria plataforma Base dos Dados, não é o enriquecimento externo opcional
    que foi descartado) tem `id_municipio`, `nome`, `sigla_uf`, `nome_uf`, `id_uf` — será
    usada só para traduzir código→nome no Silver/Gold.
- Antes de escrever o código do plano, testado localmente (venv temporária) que
  `great_expectations==1.19.0` + `pandas` + `google-cloud-bigquery` +
  `google-cloud-storage` + `google-cloud-pubsub` + `gcsfs` + `functions-framework` +
  `db-dtypes` instalam juntos sem conflito, e confirmada a API real do Great
  Expectations 1.19 (`gx.get_context(mode="ephemeral")`,
  `context.data_sources.add_pandas(...)`, `ExpectCompoundColumnsToBeUnique`, etc.)
  rodando contra DataFrames de teste — para não colocar código não testado no plano.
- Plano de execução completo escrito em
  `docs/superpowers/plans/2026-07-13-pipeline-alfabetizacao-plan.md`, com 24 tarefas
  organizadas em 8 fases (setup, common utils, bronze/batch, streaming, silver,
  gold, monitoramento, FinOps+README, vídeo), cada uma com branch própria e PR.
  Autorevisão feita: adicionado comentário explícito no `process_silver/main.py`
  do plano esclarecendo que a validação Great Expectations roda só sobre
  `municipio_integrado` (não os outros 3 datasets Silver) — trade-off consciente de
  escopo/tempo, não uma lacuna.
- Iniciada a execução do plano (skill `executing-plans`), guiada, tarefa por tarefa.
- **Task 1 concluída:** projeto GCP criado (`SEU_PROJECT_ID` = `fiapfase2`), billing
  vinculado, APIs ativadas (BigQuery, Cloud Storage, Cloud Functions, Cloud Build,
  Cloud Run, Pub/Sub, Cloud Scheduler, Cloud Logging, Cloud Monitoring).
- **Task 2 concluída:** Google Cloud CLI instalado via Homebrew (`gcloud-cli` 575.0.1).
  Autenticado como `pedrovieirarota@gmail.com` (`gcloud auth login` +
  `gcloud auth application-default login`); projeto padrão definido como `fiapfase2`
  (`gcloud config set project`); quota project do ADC corrigido para bater com o
  projeto ativo.
- **Task 3 concluída:** criados e confirmados os três recursos de infraestrutura no
  GCP: bucket GCS `gs://fiapfase2-pipeline-alfabetizacao` (região us-central1),
  dataset BigQuery `fiapfase2:gold_alfabetizacao` (região us-central1), tópico Pub/Sub
  `alfabetizacao-eventos`.
- Decisão: ingestão da camada Bronze via **query direta no BigQuery público da Base dos
  Dados** (projeto `basedosdados`), gravando o resultado como Parquet no GCS. Evita
  download manual/parsing de CSV e é mais fiel ao dado fonte, aproveitando que já
  estamos no GCP.
