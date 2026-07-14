import json
import os
import random
import time
from datetime import date

from google.cloud import pubsub_v1

# Lista de UFs usada para sortear qual estado "recebeu uma atualização" no evento
# simulado. Não precisa ser todas as 27 — é só para dar variedade nos dados de teste.
UFS = ["SP", "RJ", "MG", "BA", "CE", "PA", "PR", "RS", "PE", "AM"]


def gerar_evento_simulado() -> dict:
    """Cria um evento fake representando 'o indicador de alfabetização de uma UF
    acabou de ser atualizado'. Isso simula o tipo de evento que o desafio pede
    (atualização de indicadores/metas em tempo quase real) já que não existe uma
    fonte real emitindo esse tipo de notificação ao vivo.
    """
    return {
        "tipo_evento": "atualizacao_indicador",
        "ano": date.today().year,
        "sigla_uf": random.choice(UFS),
        "taxa_alfabetizacao": round(random.uniform(60, 95), 2),
        "timestamp": time.time(),
    }


def publicar_eventos(project_id: str, topico: str, quantidade: int = 5, intervalo_segundos: int = 2) -> None:
    """Publica `quantidade` eventos simulados no tópico Pub/Sub, um a cada
    `intervalo_segundos`, para imitar eventos chegando aos poucos (em vez de tudo de
    uma vez, o que pareceria mais com um batch do que com streaming).
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topico)

    for _ in range(quantidade):
        evento = gerar_evento_simulado()
        data = json.dumps(evento).encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(f"Publicado: {evento} -> message_id={future.result()}")
        time.sleep(intervalo_segundos)


if __name__ == "__main__":
    # Lê o PROJECT_ID de uma variável de ambiente (em vez de deixar fixo no código)
    # para o mesmo script funcionar em qualquer projeto GCP, não só o nosso.
    publicar_eventos(
        project_id=os.environ["PROJECT_ID"],
        topico="alfabetizacao-eventos",
    )
