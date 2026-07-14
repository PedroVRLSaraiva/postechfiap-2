from scripts.publish_stream_events import gerar_evento_simulado, UFS


def test_gerar_evento_simulado_tem_os_campos_esperados():
    evento = gerar_evento_simulado()

    assert set(evento.keys()) == {"tipo_evento", "ano", "sigla_uf", "taxa_alfabetizacao", "timestamp"}
    assert evento["tipo_evento"] == "atualizacao_indicador"
    assert evento["sigla_uf"] in UFS
    assert 0 <= evento["taxa_alfabetizacao"] <= 100
