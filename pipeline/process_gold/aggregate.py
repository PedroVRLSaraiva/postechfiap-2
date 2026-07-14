import pandas as pd


def comparar_meta_vs_resultado(
    df: pd.DataFrame,
    coluna_resultado: str = "taxa_alfabetizacao",
    coluna_meta: str = "meta_alfabetizacao_2024",
) -> pd.DataFrame:
    """Calcula a diferença entre o resultado real e a meta, e se a meta foi
    atingida. Isso é literalmente o dataset 'Comparação entre metas e resultados'
    citado como exemplo de camada Gold no desafio.
    """
    df = df.copy()
    df["gap_meta_resultado"] = df[coluna_resultado] - df[coluna_meta]
    df["atingiu_meta"] = df["gap_meta_resultado"] >= 0
    return df


def calcular_evolucao_temporal(
    df: pd.DataFrame,
    colunas_grupo: list,
    coluna_indicador: str = "taxa_alfabetizacao",
) -> pd.DataFrame:
    """Agrupa o indicador por ano (e por localidade, via colunas_grupo) para mostrar
    como ele mudou ao longo do tempo — o dataset 'Evolução temporal do indicador'
    citado no desafio.
    """
    return (
        df.groupby(colunas_grupo + ["ano"], as_index=False)[coluna_indicador]
        .mean()
        .sort_values(colunas_grupo + ["ano"])
        .reset_index(drop=True)
    )


def montar_comparacao_multi_nivel(
    df_municipio: pd.DataFrame,
    df_uf: pd.DataFrame,
    df_brasil: pd.DataFrame,
) -> pd.DataFrame:
    """Empilha os 3 níveis geográficos (município/UF/Brasil) numa única tabela, com
    uma coluna 'nivel_geografico' identificando qual é qual. Isso permite comparar
    os três níveis numa única consulta/gráfico, em vez de precisar de 3 tabelas
    separadas.
    """
    colunas_comuns = [
        "nivel_geografico", "localidade", "ano", "rede",
        "taxa_alfabetizacao", "meta_alfabetizacao_2024",
        "gap_meta_resultado", "atingiu_meta",
    ]

    municipio = df_municipio.assign(nivel_geografico="municipio", localidade=df_municipio["nome"])
    uf = df_uf.assign(nivel_geografico="uf", localidade=df_uf["sigla_uf"])
    brasil = df_brasil.assign(nivel_geografico="brasil", localidade="Brasil")

    return pd.concat(
        [municipio[colunas_comuns], uf[colunas_comuns], brasil[colunas_comuns]],
        ignore_index=True,
    )
