import os

import functions_framework

try:
    from pipeline.common.bq_utils import load_dataframe_to_bq
    from pipeline.common.gcs_utils import read_camada_latest
    from pipeline.process_gold.aggregate import (
        calcular_evolucao_temporal,
        comparar_meta_vs_resultado,
        montar_comparacao_multi_nivel,
    )
except ImportError:
    from aggregate import (
        calcular_evolucao_temporal,
        comparar_meta_vs_resultado,
        montar_comparacao_multi_nivel,
    )
    from bq_utils import load_dataframe_to_bq
    from gcs_utils import read_camada_latest


@functions_framework.http
def process_gold(request):
    """Cloud Function que lê os 3 datasets integrados do Silver, calcula as
    métricas analíticas finais (gap vs. meta, comparação multi-nível, evolução no
    tempo) e carrega os 3 resultados no BigQuery — a camada pronta para
    dashboards/consultas SQL. Acionada pelo Cloud Scheduler, depois do process_silver.
    """
    bucket_name = os.environ["BUCKET_NAME"]
    project_id = os.environ["PROJECT_ID"]

    municipio = read_camada_latest(bucket_name, "silver", "municipio_integrado")
    uf = read_camada_latest(bucket_name, "silver", "uf_integrado")
    brasil = read_camada_latest(bucket_name, "silver", "brasil_integrado")

    # Calcula o gap para a meta em cada um dos 3 níveis geográficos
    municipio = comparar_meta_vs_resultado(municipio)
    uf = comparar_meta_vs_resultado(uf)
    brasil = comparar_meta_vs_resultado(brasil)

    # Junta os 3 níveis numa tabela só, e calcula a evolução temporal em cima dela
    comparacao = montar_comparacao_multi_nivel(municipio, uf, brasil)
    evolucao = calcular_evolucao_temporal(comparacao, colunas_grupo=["nivel_geografico", "localidade"])

    # Carrega as 3 tabelas finais no BigQuery (dataset gold_alfabetizacao)
    load_dataframe_to_bq(municipio, project_id, "gold_alfabetizacao", "indicador_por_municipio")
    load_dataframe_to_bq(comparacao, project_id, "gold_alfabetizacao", "comparacao_meta_resultado")
    load_dataframe_to_bq(evolucao, project_id, "gold_alfabetizacao", "evolucao_temporal")

    return {"status": "ok", "linhas_indicador_por_municipio": len(municipio)}, 200
