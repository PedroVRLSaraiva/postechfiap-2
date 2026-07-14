import pandas as pd

from pipeline.process_gold.aggregate import (
    calcular_evolucao_temporal,
    comparar_meta_vs_resultado,
    montar_comparacao_multi_nivel,
)


def test_comparar_meta_vs_resultado_calcula_gap_e_flag():
    df = pd.DataFrame({"taxa_alfabetizacao": [85.0, 95.0], "meta_alfabetizacao_2024": [90.0, 90.0]})

    resultado = comparar_meta_vs_resultado(df)

    assert resultado["gap_meta_resultado"].tolist() == [-5.0, 5.0]
    assert resultado["atingiu_meta"].tolist() == [False, True]


def test_calcular_evolucao_temporal_agrupa_por_ano_e_localidade():
    df = pd.DataFrame({
        "sigla_uf": ["SP", "SP", "RJ"],
        "ano": [2023, 2024, 2023],
        "taxa_alfabetizacao": [80.0, 85.0, 70.0],
    })

    resultado = calcular_evolucao_temporal(df, colunas_grupo=["sigla_uf"], coluna_indicador="taxa_alfabetizacao")

    assert len(resultado) == 3
    linha_sp_2024 = resultado[(resultado["sigla_uf"] == "SP") & (resultado["ano"] == 2024)]
    assert linha_sp_2024["taxa_alfabetizacao"].iloc[0] == 85.0


def test_montar_comparacao_multi_nivel_junta_os_tres_niveis():
    municipio = pd.DataFrame({
        "nome": ["São Paulo"], "ano": [2024], "rede": ["municipal"],
        "taxa_alfabetizacao": [85.0], "meta_alfabetizacao_2024": [90.0],
        "gap_meta_resultado": [-5.0], "atingiu_meta": [False],
    })
    uf = pd.DataFrame({
        "sigla_uf": ["SP"], "ano": [2024], "rede": ["municipal"],
        "taxa_alfabetizacao": [83.0], "meta_alfabetizacao_2024": [88.0],
        "gap_meta_resultado": [-5.0], "atingiu_meta": [False],
    })
    brasil = pd.DataFrame({
        "ano": [2024], "rede": ["municipal"],
        "taxa_alfabetizacao": [80.0], "meta_alfabetizacao_2024": [82.0],
        "gap_meta_resultado": [-2.0], "atingiu_meta": [False],
    })

    resultado = montar_comparacao_multi_nivel(municipio, uf, brasil)

    assert set(resultado["nivel_geografico"]) == {"municipio", "uf", "brasil"}
    assert len(resultado) == 3
