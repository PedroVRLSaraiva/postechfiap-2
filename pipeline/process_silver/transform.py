import pandas as pd

# Mapeamento código -> texto da coluna "rede", confirmado consultando a tabela
# `dicionario` (bronze) do próprio dataset fonte. É o mesmo mapeamento nas 3 tabelas
# de resultado real (municipio, uf, alunos) — por isso um único dicionário serve
# para as três. As tabelas de meta (meta_alfabetizacao_*) já vêm com "rede" como
# texto (ex.: "Municipal"), não como código — por isso o cruzamento entre resultado
# real e meta só funciona DEPOIS de decodificar o resultado real para o mesmo texto.
MAPEAMENTO_REDE = {
    "0": "Total (Federal, Estadual, Municipal e Privada)",
    "1": "Federal",
    "2": "Estadual",
    "3": "Municipal",
    "4": "Privada",
    "5": "Pública (Estadual e Municipal)",
    "6": "Pública (Federal, Estadual e Municipal)",
}


def decodificar_rede(df: pd.DataFrame, coluna: str = "rede") -> pd.DataFrame:
    """Traduz o código numérico da coluna 'rede' (ex.: "3") para o texto
    correspondente (ex.: "Municipal"), usando o mapeamento fixo acima. Sem isso, o
    cruzamento com as tabelas de meta (que já usam texto) não encontra nenhuma
    correspondência — foi um bug real encontrado ao conferir os dados no BigQuery
    após o primeiro deploy do process_gold.
    """
    df = df.copy()
    df[coluna] = df[coluna].astype(str).str.strip().map(MAPEAMENTO_REDE)
    return df


def remover_linhas_duplicadas(df: pd.DataFrame, colunas_chave: list) -> pd.DataFrame:
    """Remove linhas repetidas com base numa chave (ex.: ano+id_municipio+rede),
    mantendo a primeira ocorrência. Cobre o requisito de 'verificação de duplicidade'
    da camada Silver.
    """
    return df.drop_duplicates(subset=colunas_chave, keep="first").reset_index(drop=True)


def preencher_valores_ausentes_numericos(df: pd.DataFrame, colunas_numericas: list) -> pd.DataFrame:
    """Preenche valores ausentes (NaN) usando a mediana da própria coluna — mais
    robusta que a média quando existem valores muito fora da curva (outliers).
    Cobre o requisito de 'tratamento de valores ausentes'.
    """
    df = df.copy()
    for coluna in colunas_numericas:
        df[coluna] = df[coluna].fillna(df[coluna].median())
    return df


def padronizar_id_municipio(df: pd.DataFrame, coluna: str = "id_municipio") -> pd.DataFrame:
    """Garante que o código do município sempre tenha 7 dígitos, com zeros à
    esquerda quando necessário (ex.: "355030" -> "0355030"). Isso importa porque
    esse código, se for lido como número em algum ponto, perde o zero inicial — e aí
    o cruzamento com outras tabelas pelo mesmo código quebra silenciosamente. Cobre
    o requisito de 'normalização de chaves'.
    """
    df = df.copy()
    df[coluna] = df[coluna].astype(str).str.strip().str.zfill(7)
    return df


def enriquecer_com_geografia(df_indicador: pd.DataFrame, df_geo: pd.DataFrame) -> pd.DataFrame:
    """Junta nome do município e UF (vindos da tabela de referência geográfica) na
    tabela de indicador, usando id_municipio como chave. Parte da 'integração das
    bases' exigida na camada Silver.
    """
    return df_indicador.merge(
        df_geo[["id_municipio", "nome", "sigla_uf", "nome_uf"]],
        on="id_municipio",
        how="left",
    )


def integrar_resultado_com_meta(df_resultado: pd.DataFrame, df_meta: pd.DataFrame, chaves: list) -> pd.DataFrame:
    """Junta a tabela de resultado real (ex.: taxa de alfabetização observada) com a
    tabela de meta (ex.: meta_alfabetizacao_2024) para o mesmo ano/local/rede.
    """
    return df_resultado.merge(df_meta, on=chaves, how="left", suffixes=("", "_meta"))
