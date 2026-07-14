import base64
import json
import os
from datetime import UTC, datetime

import functions_framework
import pandas as pd

# Mesmo padrão de import "tenta dos dois jeitos" usado no ingest_batch: localmente
# (pytest) existe o pacote pipeline.common; depois de deployado, só existe o arquivo
# gcs_utils.py copiado como vizinho deste main.py.
try:
    from pipeline.common.gcs_utils import write_camada_parquet
except ImportError:
    from gcs_utils import write_camada_parquet


@functions_framework.cloud_event
def ingest_stream(cloud_event):
    """Cloud Function acionada automaticamente sempre que uma mensagem chega no
    tópico Pub/Sub 'alfabetizacao-eventos'. Diferente do ingest_batch (que só roda
    quando o Cloud Scheduler chama), esta função reage a cada evento individual —
    é o que caracteriza "streaming" na prática.
    """
    bucket_name = os.environ["BUCKET_NAME"]

    # O Pub/Sub entrega a mensagem codificada em Base64 dentro de cloud_event.data.
    # Precisamos decodificar de volta para texto e depois interpretar como JSON.
    mensagem_codificada = cloud_event.data["message"]["data"]
    payload = base64.b64decode(mensagem_codificada).decode("utf-8")
    evento = json.loads(payload)

    # Transforma o evento (um dicionário) numa "tabela" de uma linha só, para poder
    # reaproveitar a mesma função write_camada_parquet usada em todo o resto da
    # pipeline (que espera um DataFrame, não um dict solto).
    df = pd.DataFrame([evento])
    agora = datetime.now(UTC)
    write_camada_parquet(
        df,
        bucket_name,
        "bronze",
        "eventos_streaming",
        data_referencia=agora.strftime("%Y-%m-%dT%H%M%S"),
    )
