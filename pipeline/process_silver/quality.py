import great_expectations as gx
import pandas as pd


def validar_dataframe(
    df: pd.DataFrame,
    nome_ativo: str,
    colunas_nao_nulas: list,
    colunas_chave_unica: list,
    coluna_faixa_valida: tuple,
) -> dict:
    """Roda checagens de qualidade sobre um DataFrame usando Great Expectations.

    Cobre os 4 pontos exigidos pelo desafio:
    - valores ausentes: uma regra "não pode ser nulo" por coluna em colunas_nao_nulas;
    - duplicidade / chave de relacionamento: uma regra "essa combinação de colunas
      tem que ser única" (colunas_chave_unica) — se duas linhas repetem a mesma
      combinação, a regra falha;
    - consistência: uma regra de faixa de valores válidos (coluna_faixa_valida),
      ex.: uma taxa/percentual não pode passar de 100.

    Devolve um dicionário simples {"sucesso": bool, "detalhes": {...}} em vez do
    objeto de resultado "cru" do Great Expectations, para ficar fácil de usar em
    outras partes do código sem precisar conhecer a API da biblioteca.
    """
    # "context" é o objeto raiz do Great Expectations. mode="ephemeral" significa
    # que ele guarda tudo só em memória (sem criar pastas/arquivos de configuração
    # no disco) — ideal para rodar dentro de uma Cloud Function, que é descartável.
    context = gx.get_context(mode="ephemeral")

    # Esses três passos "embrulham" o nosso DataFrame comum num formato que o Great
    # Expectations entende (chamado de "batch"). É um ritual da própria biblioteca:
    # 1) registra uma fonte de dados pandas, 2) diz que teremos um "asset" (uma
    # tabela) vindo de um DataFrame, 3) pega esse DataFrame como um "batch" (lote)
    # pronto para validar.
    data_source = context.data_sources.add_pandas(f"{nome_ativo}_datasource")
    data_asset = data_source.add_dataframe_asset(name=nome_ativo)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{nome_ativo}_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # "suite" é a lista de regras (expectations) que vamos checar de uma vez.
    suite = gx.ExpectationSuite(name=f"{nome_ativo}_suite")
    for coluna in colunas_nao_nulas:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=coluna))
    suite.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=colunas_chave_unica))

    coluna_faixa, valor_min, valor_max = coluna_faixa_valida
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column=coluna_faixa, min_value=valor_min, max_value=valor_max)
    )

    resultado = batch.validate(suite)

    # resultado.results é uma lista com o resultado de CADA regra individual; aqui
    # transformamos isso num dicionário simples {nome_da_regra: passou_ou_nao} para
    # facilitar a leitura de qual regra específica falhou, se falhou.
    detalhes = {r.expectation_config.type: r.success for r in resultado.results}
    return {"sucesso": resultado.success, "detalhes": detalhes}
