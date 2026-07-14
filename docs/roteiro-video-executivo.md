# Roteiro — Vídeo Executivo (até 5 minutos)

> Linguagem para liderança/stakeholders, não para outros engenheiros. Evite jargão
> técnico desnecessário (Cloud Function, Parquet, etc.) — quando precisar citar,
> explique em uma frase o que aquilo significa em termos de resultado de negócio.

## 1. O problema de negócio (~1 min)

"Garantir que toda criança brasileira esteja alfabetizada até o final do 2º ano do
ensino fundamental é uma meta nacional para 2030. Mas hoje, entender *onde* e *por
quê* isso não está acontecendo exige cruzar dados que vivem espalhados — metas
nacionais, estaduais, municipais, e os resultados reais de cada avaliação. Sem essa
integração, gestores públicos tomam decisão com informação incompleta, e
descobrem tarde demais que um município está muito abaixo da meta."

## 2. A solução: uma pipeline automatizada (~1,5 min)

"Construímos uma pipeline de dados que roda sozinha, todos os dias, na nuvem
(Google Cloud). Ela busca automaticamente os dados públicos oficiais de
alfabetização, limpa e valida a qualidade desses dados, cruza os resultados reais
com as metas de cada município e estado, e entrega um painel de indicadores pronto
para consulta — sem nenhuma intervenção manual.

Ela também é capaz de reagir a atualizações em tempo real, não só a cargas
agendadas — simulando o cenário em que um novo resultado chega e precisa ser
incorporado imediatamente à análise.

E tudo isso roda com custo de infraestrutura próximo de zero: a arquitetura foi
desenhada para caber inteiramente na camada gratuita da nuvem."

## 3. O valor para análises educacionais (~1,5 min)

"O resultado prático: qualquer gestor ou analista consegue responder, em segundos,
perguntas como 'quais municípios estão mais distantes da meta de alfabetização?' ou
'como esse indicador evoluiu nos últimos anos nesta região?' — sem precisar cruzar
planilhas manualmente.

Isso significa decisões de política pública mais rápidas e baseadas em evidência,
em vez de intuição. E como a pipeline também verifica automaticamente a qualidade
dos dados antes de liberá-los, há confiança de que a decisão está sendo tomada em
cima de números corretos, não de dados duplicados ou incompletos."

## 4. Potencial para inteligência artificial (~1 min)

"A base de dados que essa pipeline entrega já está pronta para alimentar o próximo
passo: modelos de inteligência artificial. Por exemplo, um modelo preditivo que
identifique, com antecedência, quais municípios têm maior risco de não atingir a
meta de 2030 — permitindo que o poder público intervenha antes do problema
aparecer no resultado oficial, não depois. A mesma base também permite identificar
padrões de desigualdade educacional entre regiões, orientando onde investir
recursos com mais impacto."

## Fechamento

"Em resumo: uma pipeline automatizada, de baixo custo, com qualidade de dados
garantida, que transforma dados públicos dispersos em decisão pública mais rápida
— e que já deixa o terreno pronto para o próximo passo, que é IA aplicada a
política educacional."

---

**Checklist antes de gravar:**
- [ ] Confirmar que está dentro de 5 minutos (ler o roteiro em voz alta e cronometrar)
- [ ] Pelo menos um integrante aparece apresentando
- [ ] Linguagem de negócio, não técnica
- [ ] Cobre os 4 pontos exigidos: problema, arquitetura, valor, potencial de IA
