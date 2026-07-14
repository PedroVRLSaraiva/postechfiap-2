import os

import functions_framework

try:
    from pipeline.common.gcs_utils import read_camada_latest, write_camada_parquet
    from pipeline.process_silver.quality import validar_dataframe
    from pipeline.process_silver.transform import (
        decodificar_rede,
        enriquecer_com_geografia,
        integrar_resultado_com_meta,
        padronizar_id_municipio,
        preencher_valores_ausentes_numericos,
        remover_linhas_duplicadas,
    )
except ImportError:
    from gcs_utils import read_camada_latest, write_camada_parquet
    from quality import validar_dataframe
    from transform import (
        decodificar_rede,
        enriquecer_com_geografia,
        integrar_resultado_com_meta,
        padronizar_id_municipio,
        preencher_valores_ausentes_numericos,
        remover_linhas_duplicadas,
    )


@functions_framework.http
def process_silver(request):
    """Cloud Function que transforma o Bronze cru em 4 datasets Silver limpos e
    integrados. Acionada pelo Cloud Scheduler, alguns minutos depois do ingest_batch
    (para dar tempo do Bronze do dia já estar pronto).
    """
    bucket_name = os.environ["BUCKET_NAME"]

    # 1) Lê tudo que precisamos do Bronze (sempre a versão "latest")
    uf = read_camada_latest(bucket_name, "bronze", "uf")
    municipio = read_camada_latest(bucket_name, "bronze", "municipio")
    meta_brasil = read_camada_latest(bucket_name, "bronze", "meta_alfabetizacao_brasil")
    meta_uf = read_camada_latest(bucket_name, "bronze", "meta_alfabetizacao_uf")
    meta_municipio = read_camada_latest(bucket_name, "bronze", "meta_alfabetizacao_municipio")
    alunos = read_camada_latest(bucket_name, "bronze", "alunos")
    geo = read_camada_latest(bucket_name, "bronze", "municipio_geo")

    # 2) Município: limpa, padroniza a chave, decodifica "rede" (código -> texto,
    # necessário para o cruzamento com a meta funcionar) e junta com a meta e com
    # o nome/UF
    municipio = remover_linhas_duplicadas(municipio, ["ano", "id_municipio", "rede"])
    municipio = preencher_valores_ausentes_numericos(municipio, ["taxa_alfabetizacao"])
    municipio = padronizar_id_municipio(municipio)
    municipio = decodificar_rede(municipio)
    meta_municipio = padronizar_id_municipio(meta_municipio)
    municipio_integrado = integrar_resultado_com_meta(
        municipio, meta_municipio, chaves=["ano", "id_municipio", "rede"]
    )
    municipio_integrado = enriquecer_com_geografia(municipio_integrado, geo)

    # 3) UF: mesma ideia, mas sem geografia (a própria tabela "uf" já tem sigla_uf)
    uf = remover_linhas_duplicadas(uf, ["ano", "sigla_uf", "rede"])
    uf = preencher_valores_ausentes_numericos(uf, ["taxa_alfabetizacao"])
    uf = decodificar_rede(uf)
    uf_integrado = integrar_resultado_com_meta(uf, meta_uf, chaves=["ano", "sigla_uf", "rede"])

    # 4) Brasil: essa tabela fonte já vem com resultado real + meta juntos, então só
    # precisa da limpeza básica, sem integração adicional
    brasil_integrado = remover_linhas_duplicadas(meta_brasil, ["ano", "rede"])
    brasil_integrado = preencher_valores_ausentes_numericos(brasil_integrado, ["taxa_alfabetizacao"])

    # 5) Alunos: limpeza dos microdados individuais (decodifica "rede" também, só
    # por padronização/legibilidade — esta tabela não é cruzada com nenhuma meta)
    alunos_limpo = remover_linhas_duplicadas(alunos, ["ano", "id_aluno"])
    alunos_limpo = preencher_valores_ausentes_numericos(alunos_limpo, ["proficiencia"])
    alunos_limpo = padronizar_id_municipio(alunos_limpo)
    alunos_limpo = decodificar_rede(alunos_limpo)

    # 6) Portão de qualidade: validamos com Great Expectations o dataset município
    # (o mais granular e citado explicitamente como exemplo de Gold no desafio).
    # uf_integrado/brasil_integrado/alunos_limpo já passam pela limpeza determinística
    # acima (dedup + fillna), mas não têm uma segunda checagem via GE — trade-off
    # consciente de escopo/tempo, não uma lacuna esquecida. Ampliar para os outros 3
    # datasets seria só repetir esta mesma chamada com outras colunas/chaves.
    validacao = validar_dataframe(
        municipio_integrado,
        nome_ativo="municipio_integrado",
        colunas_nao_nulas=["id_municipio", "taxa_alfabetizacao"],
        colunas_chave_unica=["ano", "id_municipio", "rede"],
        coluna_faixa_valida=("taxa_alfabetizacao", 0, 100),
    )
    if not validacao["sucesso"]:
        # Se a validação falhar, NÃO gravamos nada no Silver — melhor a pipeline
        # parar aqui (e o Cloud Monitoring, na Fase 6, vai alertar sobre essa falha)
        # do que deixar dado ruim seguir escondido para a camada Gold.
        return {"erro": "validação de qualidade falhou", "detalhes": validacao["detalhes"]}, 500

    # 7) Grava os 4 datasets integrados no Silver
    write_camada_parquet(municipio_integrado, bucket_name, "silver", "municipio_integrado")
    write_camada_parquet(uf_integrado, bucket_name, "silver", "uf_integrado")
    write_camada_parquet(brasil_integrado, bucket_name, "silver", "brasil_integrado")
    write_camada_parquet(alunos_limpo, bucket_name, "silver", "alunos_limpo")

    return {"status": "ok", "linhas_municipio_integrado": len(municipio_integrado)}, 200
