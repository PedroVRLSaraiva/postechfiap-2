from google.cloud import bigquery

# "bigquery.Client" é o objeto que sabe conversar com o BigQuery de verdade (usando
# as credenciais que configuramos com `gcloud auth application-default login`).
# Só criamos esse "Client" dentro das funções (não uma vez só lá em cima) porque
# cada chamada de Cloud Function é isolada — criar o client sempre que a função for
# usada é o padrão recomendado pelo Google para esse tipo de ambiente serverless.


def query_public_table(sql: str, project_id: str) -> "pd.DataFrame":
    """Roda uma consulta SQL no BigQuery e devolve o resultado como DataFrame.

    O `project_id` é o SEU projeto (fiapfase2) — é ele quem "paga" (mesmo que o
    custo seja zero, dentro do free tier) pela consulta, mesmo quando os dados que
    estamos lendo são públicos (projeto `basedosdados`, que é outro projeto). Todo
    comando do BigQuery precisa rodar "dentro" de algum projeto — por isso essa
    exigência de passar o project_id mesmo lendo dado de fora.
    """
    client = bigquery.Client(project=project_id)
    return client.query(sql).to_dataframe()


def load_dataframe_to_bq(
    df: "pd.DataFrame",
    project_id: str,
    dataset_id: str,
    table_id: str,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    """Grava um DataFrame como tabela dentro do BigQuery (usado pela camada Gold).

    `write_disposition="WRITE_TRUNCATE"` significa "apague o conteúdo antigo da
    tabela e coloque esse novo no lugar" — faz sentido aqui porque a cada execução
    da pipeline queremos que a tabela Gold reflita o estado mais atual, e não ficar
    empilhando linhas duplicadas de execuções antigas.
    """
    client = bigquery.Client(project=project_id)
    destino = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = client.load_table_from_dataframe(df, destino, job_config=job_config)
    job.result()  # espera a carga terminar antes de seguir em frente
