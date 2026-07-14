import pandas as pd
import pipeline.common.gcs_utils as gcs_utils


def test_write_parquet_to_gcs_monta_uri_correta(monkeypatch):
    uris_chamadas = []

    def fake_to_parquet(self, path, index=False):
        uris_chamadas.append(path)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)
    df = pd.DataFrame({"a": [1, 2]})

    gcs_utils.write_parquet_to_gcs(df, "meu-bucket", "bronze/uf/uf.parquet")

    assert uris_chamadas == ["gs://meu-bucket/bronze/uf/uf.parquet"]


def test_read_parquet_from_gcs_monta_uri_correta(monkeypatch):
    uris_chamadas = []

    def fake_read_parquet(path):
        uris_chamadas.append(path)
        return pd.DataFrame({"a": [1]})

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet)

    df = gcs_utils.read_parquet_from_gcs("meu-bucket", "bronze/uf/uf.parquet")

    assert uris_chamadas == ["gs://meu-bucket/bronze/uf/uf.parquet"]
    assert len(df) == 1


def test_write_camada_parquet_grava_duas_copias(monkeypatch):
    chamadas = []

    def fake_write(df, bucket_name, blob_path):
        chamadas.append((bucket_name, blob_path))

    monkeypatch.setattr(gcs_utils, "write_parquet_to_gcs", fake_write)
    df = pd.DataFrame({"a": [1]})

    gcs_utils.write_camada_parquet(df, "meu-bucket", "bronze", "uf", data_referencia="2026-07-14")

    assert ("meu-bucket", "bronze/uf/dt=2026-07-14/uf.parquet") in chamadas
    assert ("meu-bucket", "bronze/uf/latest/uf.parquet") in chamadas


def test_read_camada_latest_le_o_caminho_latest(monkeypatch):
    caminhos_lidos = []

    def fake_read(bucket_name, blob_path):
        caminhos_lidos.append(blob_path)
        return pd.DataFrame({"a": [1]})

    monkeypatch.setattr(gcs_utils, "read_parquet_from_gcs", fake_read)

    gcs_utils.read_camada_latest("meu-bucket", "bronze", "uf")

    assert caminhos_lidos == ["bronze/uf/latest/uf.parquet"]
