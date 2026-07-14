import pandas as pd
from pipeline.ingest_batch import main as ingest_batch


def test_ingest_batch_grava_todas_as_tabelas(monkeypatch):
    queries_rodadas = []
    tabelas_gravadas = []

    def fake_query_public_table(sql, project_id):
        queries_rodadas.append(sql)
        return pd.DataFrame({"a": [1]})

    def fake_write_camada_parquet(df, bucket_name, camada, tabela, data_referencia=None):
        tabelas_gravadas.append((camada, tabela))

    monkeypatch.setattr(ingest_batch, "query_public_table", fake_query_public_table)
    monkeypatch.setattr(ingest_batch, "write_camada_parquet", fake_write_camada_parquet)
    monkeypatch.setenv("BUCKET_NAME", "meu-bucket")
    monkeypatch.setenv("PROJECT_ID", "meu-projeto")

    resultado, status = ingest_batch.ingest_batch(request=None)

    assert status == 200
    assert len(queries_rodadas) == len(ingest_batch.TABELAS_BATCH)
    nomes_gravados = {tabela for _, tabela in tabelas_gravadas}
    assert nomes_gravados == set(ingest_batch.TABELAS_BATCH.keys())
    assert all(camada == "bronze" for camada, _ in tabelas_gravadas)
