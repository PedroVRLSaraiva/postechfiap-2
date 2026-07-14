import base64
import json

from pipeline.ingest_stream import main as ingest_stream


class FakeCloudEvent:
    """Imita o objeto que o Pub/Sub entrega para a Cloud Function — na vida real
    vem do Google, aqui a gente monta um "de mentira" só com o formato certo
    (a mensagem em si, codificada em Base64) para testar sem precisar do GCP."""

    def __init__(self, payload: dict):
        mensagem_codificada = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
        self.data = {"message": {"data": mensagem_codificada}}


def test_ingest_stream_grava_o_evento_recebido(monkeypatch):
    tabelas_gravadas = []

    def fake_write_camada_parquet(df, bucket_name, camada, tabela, data_referencia=None):
        tabelas_gravadas.append((camada, tabela, df.to_dict("records")))

    monkeypatch.setattr(ingest_stream, "write_camada_parquet", fake_write_camada_parquet)
    monkeypatch.setenv("BUCKET_NAME", "meu-bucket")

    evento = {
        "tipo_evento": "atualizacao_indicador",
        "ano": 2026,
        "sigla_uf": "SP",
        "taxa_alfabetizacao": 88.5,
        "timestamp": 123.0,
    }
    cloud_event = FakeCloudEvent(evento)

    ingest_stream.ingest_stream(cloud_event)

    assert len(tabelas_gravadas) == 1
    camada, tabela, linhas = tabelas_gravadas[0]
    assert camada == "bronze"
    assert tabela == "eventos_streaming"
    assert linhas[0]["sigla_uf"] == "SP"
