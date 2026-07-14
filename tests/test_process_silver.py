import pandas as pd

from pipeline.process_silver import main as process_silver


def _bronze_fake(nome_tabela):
    """Monta versões bem pequenas (1 linha) de cada tabela do Bronze, só o
    suficiente para testar se process_silver liga as peças certas — não testa a
    qualidade dos dados em si (isso já foi testado em test_silver_quality.py)."""
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
