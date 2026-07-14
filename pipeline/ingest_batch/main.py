import os

import functions_framework

from pipeline.common.bq_utils import query_public_table
from pipeline.common.gcs_utils import write_camada_parquet

# TABELAS_BATCH mapeia "nome da tabela na nossa pipeline" -> "query SQL que busca ela
# no projeto público basedosdados". Guardar assim (em vez de repetir a mesma lógica
# 8 vezes) permite que a função abaixo processe todas com um único loop.
#
# Cada query seleciona só as colunas que a pipeline realmente usa mais na frente
# (Silver/Gold) — não usamos "SELECT *" de propósito: o BigQuery cobra por volume de
# dado lido por coluna, então pedir só o necessário já é uma economia de custo real
# (falamos mais sobre isso na seção de FinOps do README).
TABELAS_BATCH = {
    # Resultado real de alfabetização por UF (uma linha por ano/UF/rede de ensino)
    "uf": """
        SELECT ano, sigla_uf, serie, rede, taxa_alfabetizacao, media_portugues,
               proporcao_aluno_nivel_0, proporcao_aluno_nivel_1, proporcao_aluno_nivel_2,
               proporcao_aluno_nivel_3, proporcao_aluno_nivel_4, proporcao_aluno_nivel_5,
               proporcao_aluno_nivel_6, proporcao_aluno_nivel_7, proporcao_aluno_nivel_8
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf`
    """,
    # Resultado real de alfabetização por município
    "municipio": """
        SELECT ano, id_municipio, serie, rede, taxa_alfabetizacao, media_portugues,
               proporcao_aluno_nivel_0, proporcao_aluno_nivel_1, proporcao_aluno_nivel_2,
               proporcao_aluno_nivel_3, proporcao_aluno_nivel_4, proporcao_aluno_nivel_5,
               proporcao_aluno_nivel_6, proporcao_aluno_nivel_7, proporcao_aluno_nivel_8
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.municipio`
    """,
    # Meta nacional de alfabetização (essa tabela já traz resultado real + metas juntos)
    "meta_alfabetizacao_brasil": """
        SELECT ano, rede, taxa_alfabetizacao, meta_alfabetizacao_2024, meta_alfabetizacao_2025,
               meta_alfabetizacao_2026, meta_alfabetizacao_2027, meta_alfabetizacao_2028,
               meta_alfabetizacao_2029, meta_alfabetizacao_2030, percentual_participacao
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil`
    """,
    # Meta de alfabetização por UF
    "meta_alfabetizacao_uf": """
        SELECT ano, sigla_uf, rede, taxa_alfabetizacao, meta_alfabetizacao_2024, meta_alfabetizacao_2025,
               meta_alfabetizacao_2026, meta_alfabetizacao_2027, meta_alfabetizacao_2028,
               meta_alfabetizacao_2029, meta_alfabetizacao_2030, percentual_participacao
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf`
    """,
    # Meta de alfabetização por município
    "meta_alfabetizacao_municipio": """
        SELECT ano, id_municipio, rede, taxa_alfabetizacao, meta_alfabetizacao_2024, meta_alfabetizacao_2025,
               meta_alfabetizacao_2026, meta_alfabetizacao_2027, meta_alfabetizacao_2028,
               meta_alfabetizacao_2029, meta_alfabetizacao_2030, nivel_alfabetizacao, percentual_participacao
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio`
    """,
    # Microdados individuais por aluno avaliado (a entidade "Dados de alunos" do desafio)
    "alunos": """
        SELECT ano, id_municipio, id_escola, id_aluno, serie, rede, presenca,
               alfabetizado, proficiencia, peso_aluno
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.alunos`
    """,
    # Tabela auxiliar que traduz códigos/categorias (ex.: valores de "rede") em texto
    "dicionario": """
        SELECT id_tabela, nome_coluna, chave, cobertura_temporal, valor
        FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    """,
    # Tabela de referência geográfica (nome do município e UF por extenso) — vem de
    # um dataset diferente (br_bd_diretorios_brasil), mantido pela própria Base dos
    # Dados para todos os projetos, não é o enriquecimento externo opcional do desafio
    "municipio_geo": """
        SELECT id_municipio, nome, sigla_uf, nome_uf, id_uf
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    """,
}


@functions_framework.http
def ingest_batch(request):
    """Cloud Function acionada pelo Cloud Scheduler (batch diário).

    Para cada tabela em TABELAS_BATCH: roda a query no BigQuery público, e grava o
    resultado cru (sem nenhuma transformação) na camada Bronze do bucket. Devolve
    quantas linhas cada tabela trouxe, só para conferência rápida.
    """
    bucket_name = os.environ["BUCKET_NAME"]
    project_id = os.environ["PROJECT_ID"]

    linhas_por_tabela = {}
    for tabela, sql in TABELAS_BATCH.items():
        df = query_public_table(sql, project_id=project_id)
        write_camada_parquet(df, bucket_name, "bronze", tabela)
        linhas_por_tabela[tabela] = len(df)

    return linhas_por_tabela, 200
