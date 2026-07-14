from datetime import date

import pandas as pd


def write_parquet_to_gcs(df: pd.DataFrame, bucket_name: str, blob_path: str) -> None:
    uri = f"gs://{bucket_name}/{blob_path}"
    df.to_parquet(uri, index=False)


def read_parquet_from_gcs(bucket_name: str, blob_path: str) -> pd.DataFrame:
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

    A cópia versionada preserva o histórico completo (exigido para a camada Bronze);
    a cópia 'latest' permite que a próxima camada sempre leia o dado mais recente
    sem precisar descobrir qual é a partição de data mais nova.
    """
    data_referencia = data_referencia or date.today().isoformat()
    write_parquet_to_gcs(df, bucket_name, f"{camada}/{tabela}/dt={data_referencia}/{tabela}.parquet")
    write_parquet_to_gcs(df, bucket_name, f"{camada}/{tabela}/latest/{tabela}.parquet")


def read_camada_latest(bucket_name: str, camada: str, tabela: str) -> pd.DataFrame:
    return read_parquet_from_gcs(bucket_name, f"{camada}/{tabela}/latest/{tabela}.parquet")
