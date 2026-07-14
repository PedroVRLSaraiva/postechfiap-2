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
