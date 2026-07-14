import pandas as pd
import pipeline.common.bq_utils as bq_utils


class FakeQueryJob:
    def __init__(self, df):
        self._df = df

    def to_dataframe(self):
        return self._df


class FakeBQClient:
    def __init__(self, project=None):
        self.project = project
        self.queries_recebidas = []
        self.cargas_recebidas = []

    def query(self, sql):
        self.queries_recebidas.append(sql)
        return FakeQueryJob(pd.DataFrame({"a": [1, 2]}))

    def load_table_from_dataframe(self, df, destino, job_config=None):
        self.cargas_recebidas.append((df, destino, job_config))

        class FakeLoadJob:
            def result(self_inner):
                return None

        return FakeLoadJob()


def test_query_public_table_roda_o_sql_recebido(monkeypatch):
    fake_client = FakeBQClient()
    monkeypatch.setattr(bq_utils.bigquery, "Client", lambda project=None: fake_client)

    df = bq_utils.query_public_table("SELECT * FROM tabela", project_id="meu-projeto")

    assert fake_client.queries_recebidas == ["SELECT * FROM tabela"]
    assert len(df) == 2


def test_load_dataframe_to_bq_monta_destino_correto(monkeypatch):
    fake_client = FakeBQClient()
    monkeypatch.setattr(bq_utils.bigquery, "Client", lambda project=None: fake_client)

    df = pd.DataFrame({"a": [1]})
    bq_utils.load_dataframe_to_bq(df, "meu-projeto", "gold_alfabetizacao", "indicador_por_municipio")

    assert len(fake_client.cargas_recebidas) == 1
    df_carregado, destino, _ = fake_client.cargas_recebidas[0]
    assert destino == "meu-projeto.gold_alfabetizacao.indicador_por_municipio"
