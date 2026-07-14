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
