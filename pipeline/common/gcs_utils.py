from datetime import date

import pandas as pd

# GCS = "Google Cloud Storage": é o "HD na nuvem" do Google. A gente não usa a
# biblioteca do GCS diretamente aqui — o pandas já sabe conversar com o GCS sozinho
# se a gente passar um caminho que começa com "gs://" (desde que o pacote `gcsfs`
# esteja instalado, que já está no requirements.txt). Por isso as funções abaixo são
# tão curtas: a maior parte do trabalho pesado já é feita pelo pandas.


def write_parquet_to_gcs(df: pd.DataFrame, bucket_name: str, blob_path: str) -> None:
    """Salva um DataFrame como arquivo Parquet dentro de um bucket do GCS.

    Parquet é um formato de arquivo binário (não é texto como CSV) otimizado para
    leitura rápida e ocupa menos espaço — é o formato usado em todas as camadas
    (bronze/silver/gold) por ser mais barato e rápido de processar que CSV.
    """
    uri = f"gs://{bucket_name}/{blob_path}"
    df.to_parquet(uri, index=False)


def read_parquet_from_gcs(bucket_name: str, blob_path: str) -> pd.DataFrame:
    """Lê um arquivo Parquet do GCS e devolve como DataFrame (o inverso da função acima)."""
    uri = f"gs://{bucket_name}/{blob_path}"
    return pd.read_parquet(uri)


def write_camada_parquet(
    df: pd.DataFrame,
    bucket_name: str,
    camada: str,
    tabela: str,
    data_referencia: str | None = None,
) -> None:
    """Grava df em duas cópias: uma versionada por data (histórico) e uma 'latest'.

    Por quê duas cópias? O desafio pede que a camada Bronze preserve o histórico
    completo (cada execução gera uma "foto" nova, sem apagar as anteriores) — por
    isso a cópia com "dt=AAAA-MM-DD/" no caminho. Mas as próximas camadas (silver,
    gold) sempre precisam só do dado mais recente para processar, e não deveriam
    precisar "adivinhar" qual é a data mais nova — por isso também gravamos uma
    segunda cópia sempre no mesmo caminho fixo "latest/", que é sobrescrita a cada
    execução.
    """
    data_referencia = data_referencia or date.today().isoformat()
    write_parquet_to_gcs(df, bucket_name, f"{camada}/{tabela}/dt={data_referencia}/{tabela}.parquet")
    write_parquet_to_gcs(df, bucket_name, f"{camada}/{tabela}/latest/{tabela}.parquet")


def read_camada_latest(bucket_name: str, camada: str, tabela: str) -> pd.DataFrame:
    """Lê sempre a versão mais recente ('latest') de uma tabela de uma camada."""
    return read_parquet_from_gcs(bucket_name, f"{camada}/{tabela}/latest/{tabela}.parquet")
