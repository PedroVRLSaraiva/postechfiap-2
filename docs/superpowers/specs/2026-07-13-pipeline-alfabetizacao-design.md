# Design — Pipeline Híbrido para Análise da Alfabetização no Brasil

**Data:** 2026-07-13
**Prazo de entrega:** 25 horas a partir da aprovação deste spec
**Autor:** Pedro (solo), com Claude Code como par de desenvolvimento

## Contexto

Tech Challenge Fase 2 (FIAP): construir uma pipeline de dados híbrida (batch +
streaming) em nuvem, seguindo a Arquitetura Medalhão (Bronze/Silver/Gold), para
integrar dados do "Indicador Criança Alfabetizada" (Base dos Dados / INEP) e apoiar
análises sobre alfabetização infantil no Brasil. Detalhes completos do enunciado em
`docs/[IAST] - Tech Challenge - Fase 2.pdf`.

**Princípio orientador deste projeto: simplicidade.** Cada peça da arquitetura e cada
passo do plano de execução deve ser o mais simples possível — o suficiente para que
alguém sem experiência prévia em engenharia de dados ou GCP consiga executar, desde
que siga a explicação de cada passo. Sempre que houver a opção entre "mais robusto" e
"mais simples", a decisão default é pela simplicidade, a menos que a robustez seja
estritamente obrigatória para atender ao enunciado do desafio.

## Decisões-base (já validadas)

- **Cloud:** GCP, usando free tier.
- **Engine de processamento:** Pandas + Great Expectations (não Spark), porque foi o
  que foi ensinado em aula (`docs/FIAP_Fase2_DataPrepare_Transcricoes.txt`) e o volume
  de dados do desafio (indicadores por UF/município) não justifica Spark.
- **Fonte dos dados:** query direta no dataset público `basedosdados` no BigQuery
  (sem download manual de CSV).
- **Streaming:** Pub/Sub (equivalente gerenciado do Kafka ensinado em aula).
- **Escopo:** Cenário C — implementação serverless completa e automatizada dos itens
  obrigatórios + o opcional de monitoramento. Sem enriquecimento externo, sem modelo
  de ML treinado (aplicação em IA fica só documentada no README).
- **Equipe:** solo. Ainda assim, uso de branches e Pull Requests é obrigatório pelo
  enunciado.

## Arquitetura

```
Base dos Dados (BigQuery público "basedosdados")
        │
        ├── BATCH: Cloud Scheduler (cron) ──► Cloud Function "ingest_batch"
        │         (metas nacionais/UF/município, dados de município, UF)
        │
        └── STREAMING: script publisher ──► Pub/Sub topic ──► Cloud Function "ingest_stream"
                  (simula eventos: atualização de indicador/meta/resultado)
                                    │
                                    ▼
                         BRONZE (GCS, Parquet, raw, particionado por data/fonte)
                                    │
                    Cloud Function "process_silver"
                    (Pandas + Great Expectations: limpeza, tipos, chaves
                    normalizadas, integração das 6 entidades)
                                    │
                                    ▼
                         SILVER (GCS, Parquet, particionado)
                                    │
                    Cloud Function "process_gold" (Pandas: agregações)
                                    │
                                    ▼
                    GOLD (BigQuery: indicador por município, metas vs.
                    resultados, evolução temporal)
                                    │
                                    ▼
                    Cloud Logging + Cloud Monitoring (métricas, alertas)
```

## Componentes

| Componente | O que faz | Por quê |
|---|---|---|
| `ingest_batch` (Cloud Function) | Acionada por Cloud Scheduler; faz query nas tabelas do `basedosdados` e grava Parquet cru no GCS (bronze) | Ingestão periódica dos dados históricos/estruturais (metas, município, UF) |
| `publish_stream_events.py` (script) | Publica eventos simulados (ex: nova medição de indicador) num tópico Pub/Sub | Simula a chegada de dados "quase em tempo real", como pede o desafio |
| `ingest_stream` (Cloud Function) | Acionada automaticamente por mensagem no Pub/Sub; grava o evento no bronze | Demonstra o lado streaming da ingestão híbrida, com trigger real (não manual) |
| `process_silver` (Cloud Function) | Acionada por Cloud Scheduler (alguns minutos depois do `ingest_batch`); lê o bronze, valida com Great Expectations, limpa, padroniza e integra as 6 entidades | Camada intermediária de qualidade e integração, exigida pela Arquitetura Medalhão |
| `process_gold` (Cloud Function) | Acionada por Cloud Scheduler (alguns minutos depois do `process_silver`); agrega o silver em datasets analíticos e carrega no BigQuery | Camada analítica final, pronta para consulta/dashboards |
| `pipeline/common/` (módulo Python) | Funções compartilhadas de leitura/escrita GCS e BigQuery, usadas pelas 4 functions acima | Evita duplicar código entre as functions |

**Orquestração:** em vez de encadear as functions por eventos (mais complexo de
configurar e explicar), usamos três jobs do Cloud Scheduler com horários escalonados
(ex.: `ingest_batch` às XX:00, `process_silver` às XX:10, `process_gold` às XX:20) —
simples de entender e de depurar por quem não tem experiência prévia com
orquestração de pipelines.

## Qualidade de dados

Great Expectations roda na etapa Silver, com uma expectation suite simples por
entidade cobrindo os 4 pontos pedidos no desafio: duplicidade, valores ausentes,
validação de chaves de relacionamento, consistência entre tabelas. Se uma validação
falhar, o dado fica retido no silver (não avança para o gold) e é registrado um log
de erro — que o Monitoring capta.

## Monitoramento (opcional, incluído)

Cloud Logging registra execução/erros de cada Cloud Function. Cloud Monitoring cria
alertas simples por e-mail para: falha de execução de qualquer function, e ausência
de execução no horário esperado do batch. Mantido no nível mais simples possível
(sem dashboards customizados elaborados).

## FinOps

- Armazenamento em Parquet, particionado por data/fonte no GCS.
- Queries no BigQuery restritas a colunas/partições necessárias.
- Cloud Functions com memória/timeout mínimos configurados.
- Estimativa de custo (esperada: R$0, dentro do free tier) documentada no README.

## Repositório e Git

- Branch `main` protegida; uma branch por funcionalidade (ex.:
  `feature/bronze-ingestion`, `feature/silver-quality`, `feature/gold-layer`,
  `feature/streaming`, `feature/monitoring`).
- Commits pequenos e descritivos, refletindo a evolução real do trabalho.
- Uma Pull Request por branch, com uma descrição curta do que mudou e por quê antes
  do merge para `main` (mesmo trabalhando solo, para atender a exigência do
  enunciado).

## Estrutura do repositório

```
├── docs/
│   ├── [IAST] - Tech Challenge - Fase 2.pdf
│   ├── FIAP_Fase2_DataPrepare_Transcricoes.txt
│   └── superpowers/specs/  (specs de design)
├── pipeline/
│   ├── common/          (funções compartilhadas GCS/BigQuery)
│   ├── ingest_batch/
│   ├── ingest_stream/
│   ├── process_silver/
│   └── process_gold/
├── scripts/
│   └── publish_stream_events.py
├── great_expectations/   (expectation suites)
├── logbook.md
└── README.md
```

## Fora de escopo (documentado, não implementado)

- Enriquecimento com fontes externas (IBGE, INEP Censo Escolar, etc.).
- Treinamento de modelo de ML — a seção "Aplicação em IA" do README explica o
  potencial de uso da camada Gold sem implementar o modelo.

## Como este spec deve ser usado no plano de execução

O próximo passo é transformar este design num plano de execução passo a passo
(via skill `writing-plans`). Cada passo do plano deve:
1. Ser pequeno e simples o suficiente para ser executado isoladamente.
2. Vir acompanhado de uma explicação simples do **o quê** está sendo feito e
   **por quê** — assumindo que quem executa pode não ter experiência prévia com
   GCP, Python ou engenharia de dados.
3. Ser registrado no `logbook.md` conforme for concluído.
