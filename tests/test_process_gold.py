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
