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
- Usuário pediu para eu explicar cada arquivo novo antes de escrever o código, e
  comentar o código-fonte — ajuste de estilo de comunicação salvo em memória (não é
  o padrão default de "sem comentários"), válido para o resto deste projeto.
- **Task 5 concluída:** `pipeline/common/gcs_utils.py` implementado com TDD (4 testes)
  — funções de leitura/escrita de Parquet no GCS com convenção de camadas
  (`dt=`/`latest`), comentado.
- **Task 6 concluída:** `pipeline/common/bq_utils.py` implementado com TDD (2 testes)
  — query no BigQuery público e carga de DataFrame como tabela. Branch
  `feature/common-utils` (PR #2) mergeada em `main` (branch mantida, sem
  `--delete-branch`).
- **Task 7 concluída:** `pipeline/ingest_batch/main.py` implementado com TDD (1 teste)
  — query nas 8 tabelas fonte (as 6 exigidas + `dicionario` + tabela de geografia
  `br_bd_diretorios_brasil.municipio`), com seleção explícita de colunas (não
  `SELECT *`) por FinOps.
- **Task 8 concluída — deploy de `ingest-batch`:** durante o deploy real, a API
  `run.googleapis.com` precisou ser ativada manualmente (não estava habilitada apesar
  de constar como ativada no console na Task 1). Depois, dois problemas reais
  encontrados e corrigidos:
  - Estouro de memória: a tabela `alunos` tem ~3,87 milhões de linhas (~256 MB
    comprimidos no BigQuery); como DataFrame do pandas isso passa de 512 MiB. Corrigido
    aumentando a função para `--memory=2Gi`.
    Preventivamente, `process-silver` (que também lê `alunos`) foi ajustado no plano
    para os mesmos 2Gi/540s, já que sofreria do mesmo problema.
  - Timeout: com mais memória, a função ainda estourava `--timeout=300s` processando
    as 8 tabelas em sequência. Corrigido para `--timeout=540s`.
  - Teste manual final: as 8 tabelas processadas com sucesso
    (`alunos: 3.867.999 linhas`, `municipio: 23.995`, `municipio_geo: 5.571`, etc.),
    confirmado no bucket com a estrutura `dt=`/`latest` funcionando.
  - Também descoberto (e usado para dimensionar a memória): contagem/tamanho real das
    tabelas via `INFORMATION_SCHEMA.__TABLES__` do BigQuery.
- **Task 9 concluída:** job `job-ingest-batch` criado no Cloud Scheduler (cron
  `0 3 * * *`, horário UTC). Descoberto que o Cloud Scheduler tem um "attempt deadline"
  padrão de só 3 minutos para alvos HTTP — menor que os 540s que a função pode levar —
  corrigido com `--attempt-deadline=540s`. Execução forçada testada com sucesso: todas
  as 8 tabelas atualizadas no bucket, sem erros nos logs da function nem do job.
- **Tasks 10-12 concluídas (Fase 3, Streaming):** `scripts/publish_stream_events.py`
  (publisher com TDD) e `pipeline/ingest_stream/main.py` (Cloud Function consumidora,
  TDD) implementados. Deploy real exigiu ativar mais uma API (`eventarc.googleapis.com`,
  usada por triggers de evento como Pub/Sub em Cloud Functions Gen2). Teste real:
  publicados 5 eventos simulados, todos consumidos automaticamente pela função,
  gerando 5 partições `dt=` distintas (histórico preservado) + `latest/` atualizado,
  sem erros nos logs.
- **Tasks 13-16 concluídas (Fase 4, Silver):** `transform.py` (limpeza/integração,
  5 testes), `quality.py` (Great Expectations, 2 testes) e `main.py` (glue com
  portão de qualidade, 2 testes) implementados com TDD — suíte completa em 18 testes,
  todos passando. Deploy real com `--memory=2Gi --timeout=540s` (mesmo ajuste do
  ingest-batch, por causa da tabela `alunos`) funcionou de primeira. Cloud Scheduler
  `job-process-silver` criado (`10 3 * * *`, 10min depois do batch). Teste manual:
  4 tabelas Silver geradas (`municipio_integrado`: 23.995 linhas, batendo com a
  contagem real da fonte), sem erros nos logs.
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
- **Task 4 concluída:** estrutura de pastas criada (`pipeline/{common,ingest_batch,
  ingest_stream,process_silver,process_gold}`, `scripts/`, `tests/`). Venv recriada
  com Python 3.12 (a instalação do `gcloud-cli` via Homebrew havia trazido Python 3.14
  como padrão, incompatível com `great_expectations==1.19.0`, que exige `<3.14`).
  `requirements.txt` raiz instalado sem conflitos. Branch `chore/setup-gcp-and-repo`
  criada, commitada, PR #1 aberta e mergeada em `main` (mantendo a branch, por
  preferência do usuário — não deletar branches após merge neste projeto).
- Decisão: ingestão da camada Bronze via **query direta no BigQuery público da Base dos
  Dados** (projeto `basedosdados`), gravando o resultado como Parquet no GCS. Evita
  download manual/parsing de CSV e é mais fiel ao dado fonte, aproveitando que já
  estamos no GCP.
- **Nota:** as entradas deste logbook não estão 100% em ordem cronológica de arquivo
  (algumas foram inseridas no meio por engano, ancoradas num ponto fixo do texto em
  vez do fim real do arquivo). O conteúdo está completo, só a ordem de leitura no
  arquivo não reflete perfeitamente a ordem real dos eventos. A partir daqui, novas
  entradas são sempre acrescentadas no fim de verdade.
- **Bug real encontrado e corrigido durante a Task 19 (deploy de process-gold):**
  ao consultar `fiapfase2.gold_alfabetizacao.indicador_por_municipio` no BigQuery
  depois do primeiro deploy, `meta_alfabetizacao_2024` vinha sempre `null`. Investigado
  comparando `SELECT DISTINCT ano, rede` nas tabelas `municipio` (rede = código
  numérico: "0","2","3","5") e `meta_alfabetizacao_municipio` (rede = texto:
  "Municipal") — os dois formatos nunca batiam no cruzamento. Confirmado o
  mapeamento código→texto consultando a tabela `dicionario`
  (`0=Total, 1=Federal, 2=Estadual, 3=Municipal, 4=Privada, 5=Pública (Estadual e
  Municipal), 6=Pública (Federal, Estadual e Municipal)` — igual nas 3 tabelas de
  resultado real: `municipio`, `uf`, `alunos`). Corrigido adicionando
  `decodificar_rede()` em `pipeline/process_silver/transform.py`, chamada antes de
  `integrar_resultado_com_meta` para `municipio`, `uf` e `alunos`. Testes atualizados
  para usar códigos numéricos realistas nos dados fake (em vez de texto direto).
  Reimplantado `process-silver` e `process-gold`, e reconferido no BigQuery: a
  rede "Municipal" agora cruza corretamente com a meta (10.464 de 10.896 linhas
  dessa rede têm meta preenchida; as demais redes — Estadual, Pública, Total — não
  têm meta mesmo, pois o programa "Compromisso Nacional Criança Alfabetizada" só
  define metas para a rede Municipal, então os `null` restantes são esperados, não
  um bug).
- **Task 19 concluída:** deploy de `process-gold` (memória/timeout padrão do plano,
  512Mi/300s, suficiente pois esta função não lê a tabela `alunos`). Cloud Scheduler
  `job-process-gold` criado (`20 3 * * *`). Após a correção do bug de `rede` acima,
  reconfirmado com sucesso: 3 tabelas analíticas carregadas no BigQuery
  (`indicador_por_municipio`, `comparacao_meta_resultado`, `evolucao_temporal`), sem
  erros nos logs.
- **Task 20 concluída:** instalado o componente `alpha` do gcloud CLI (necessário
  para os comandos de Monitoring). Criado canal de notificação por e-mail
  (`pedrovieirarota@gmail.com`) e política de alerta
  (`monitoring/alert-policy-falhas.json`) que dispara quando qualquer uma das 4
  Cloud Functions (`ingest-batch`, `ingest-stream`, `process-silver`, `process-gold`)
  registra um log de severidade ERROR. Confirmado ativo (`enabled: True`).
- **Task 21 concluída:** teste do alerta forçando uma falha real. Primeira tentativa
  de "quebrar" a função (`gcloud functions deploy ... --update-env-vars=...` sem
  `--source`/`--entry-point`) falhou silenciosamente — o comando deu erro de
  validação e a função continuou com a config antiga, então rodou a ingestão real
  várias vezes sem erro (só desperdiçou tempo, sem causar problema). Corrigido
  refazendo o deploy com todos os parâmetros. Com o `BUCKET_NAME` realmente apontando
  para um bucket inexistente, a execução falhou rápido (HTTP 500, ~1.6s) e o log de
  erro foi confirmado via `severity>=ERROR`. E-mail de alerta chegou com sucesso na
  caixa de entrada, confirmando o monitoramento ponta a ponta. Configuração revertida
  para o bucket correto logo em seguida.
