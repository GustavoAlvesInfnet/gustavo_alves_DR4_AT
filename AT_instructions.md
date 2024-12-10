# Especificação da Solução

Vamos desenvolver uma solução que utilize IA para processar informações da Câmara dos Deputados do Brasil, tanto textuais quanto imagens. A solução possui uma parte offline e online, descritas abaixo:

**Offline (dataprep.py)**: Coleta das informações com as APIs públicas da câmara e execução de atividades com prompts para sumarização, análises e marcação com palavras-chave. Criação de bases vetoriais para referência posterior.

**Online (dashboard.py)**: Visualização das informações coletadas, suas respectivas análises e interfaces com o usuário através de LLMs.

- URL base: 'https://dadosabertos.camara.leg.br/api/v2/'
- Data referência: início em 01/08/2024 e fim em 30/08/2024 (datas inclusivas).
- Plug-and-play: a solução deve esperar um arquivo .env com a chave Gemini do usuário (o aluno desenvolve com a sua, mas não a exporta para o git!).

Faça o upload de todos os arquivos finais (parquet, json, yaml e png).

## Exercício 1: Arquitetura da Solução

Desenhe a arquitetura da solução com o programa da sua escolha. A arquitetura deve indicar os pontos de processamento de informação, LLMs utilizados, bases de dados (parquets, jsons e faiss), arquivos de configuração (yaml), abas do dashboard e suas funcionalidades.

1. Exporte a arquitetura para o arquivo pdf importado no sistema.
2. Descreva a arquitetura, explicando seus pontos importantes.
3. Descreva o funcionamento de LLMs e como isso pode ser utilizado para atividades de sumarização.

## Exercício 2: Criação de Textos com LLMs

Utilize a sua conta no “poe.com” para gerar um texto curto (2 parágrafos) que explique a Câmara dos Deputados. Execute o mesmo prompt com 3 LLMs diferentes (claude, gemini e chatgpt) e:

1. Explique as vantagens e desvantagens dos três LLMs escolhidos.
2. Argumente sobre a diferença entre a resposta dos 3 LLMs.
3. Justifique a escolha da resposta final.
4. Atualize o prompt do LLM final para gerar um arquivo data/config.yaml com a resposta final (chave: overview_summary).

## Exercício 3: Processamento dos Dados de Deputados

Implemente em dataprep.py uma função que faça a coleta das informações dos deputados atuais da câmara dos deputados:

1. Colete e salve os dados dos deputados atuais da câmara no arquivo data/deputados.parquet através da url: url_base+/deputados.
2. Execute um prompt para criar o código que gere um gráfico de pizza com o total e o percentual de deputados de cada partido, salvo em 'docs/distribuicao_deputados.png'.
3. Execute um prompt utilizando os resultados da análise anterior (distribuição de deputados por partido) para gerar insights sobre a distribuição de partidos e como isso influencia a câmara. Utilize os elementos de prompts dados, persona e exemplos para instruir o LLM. Explique o objetivo de cada elemento, avalie a resposta e salve-a em data/insights_distribuicao_deputados.json.

## Exercício 4: Processamento dos Dados de Despesas

Implemente em dataprep.py uma função que colete as informações das despesas dos deputados atuais da câmara dos deputados no período de referência da solução (use a url: url_base+/deputados/{id}/despesas).

1. Agrupe os dados de despesas por dia, deputado e tipo de despesa e salve num arquivo parquet (data/serie_despesas_diárias_deputados.parquet).
2. Utilizando a técnica de prompt-chaining, crie um prompt que instrua o LLM a gerar um código python que analise os dados das despesas dos deputados. Peça ao LLM até 3 análises. Indique ao LLM quais dados estão disponíveis e o respectivo arquivo (salvo em a)) e execute as análises.
3. Utilize os resultados das 3 análises para criar um prompt usando a técnica de Generated Knowledge para instruir o LLM a gerar insights. Salve o resultado como um JSON (data/insights_despesas_deputados.json).

## Exercício 5: Processamento dos Dados de Proposições

Implemente em dataprep.py uma função que faça a coleta das informações das proposições que tramitam no período de referência (dataInicio e dataFim) e são do tema 'Economia', 'Educação' e 'Ciência, Tecnologia e Inovação' (códigos [40, 46, 62]).

1. Coletar um total de 10 proposições por tema e salvar em data/proposicoes_deputados.parquet.
2. Utilize a sumarização por chunks para resumir as proposições tramitadas no período de referência. Avalie a resposta e salve-a em data/sumarizacao_proposicoes.json.

## Exercício 6: Dashboards com Chain-of-Thoughts

Utilize 3 etapas de Chain-of-Thought prompting para escrever o código inicial do dashboard, destacando as abas Overview, Despesas e Proposições. Explique o objetivo de cada prompt na evolução do código até o arquivo dashboard.py final:

1. A aba Overview deve possuir um título e descrição da solução de sua escolha.
2. O painel deve mostrar o texto sumarizado em config.yaml.
3. O painel deve mostrar o gráfico de barras em docs/distribuicao_deputados.png.
4. O painel deve mostrar os insights do LLM sobre a distribuição de deputados em data/insights_distribuicao_deputados.json.

## Exercício 7: Dashboards com Batch-prompting

Utilize a técnica de Batch-prompting para escrever o código streamlit que preencha as abas Despesas e Proposições do código em dashboard.py. O prompt deve descrever com detalhes cada aba para geração de:

1. Aba Despesas deve mostrar os insights sobre as despesas dos deputados (data/insights_despesas_deputados.json).
2. Aba Despesas deve conter um st.selectbox para seleção do deputado.
3. Aba Despesas deve mostrar gráfico de barras com a série temporal de despesas do deputado selecionado (data/serie_despesas_diárias_deputados.parquet).
4. O painel deve mostrar uma tabela com os dados das proposições (data/proposicoes_deputados.parquet).
5. O painel deve mostrar o resumo das proposições em (data/sumarizacao_proposicoes.json).

Compare o resultado dos códigos gerados pelas técnicas de Chain-of-Thoughts e Batch-prompting.

## Exercício 8: Assistente Online com Base Vetorial

Adicione ao código da aba Proposições uma interface para chat com um assistente virtual especialista em câmara dos deputados. As informações coletadas dos deputados, despesas e proposições (e suas sumarizações) devem ser vetorizadas usando o modelo "neuralmind/bert-base-portuguese-cased" para armazenamento na base vetorial FAISS. O prompt do sistema para o assistente virtual deve ser feito com a técnica Self-Ask:

1. Explique como a técnica de self-ask pode ser utilizada nesse contexto.
2. Avalie o resultado do modelo para as seguintes perguntas:
   - Qual é o partido político com mais deputados na câmara?
   - Qual é o deputado com mais despesas na câmara?
   - Qual é o tipo de despesa mais declarada pelos deputados da câmara?
   - Quais são as informações mais relevantes sobre as proposições que falam de Economia?
   - Quais são as informações mais relevantes sobre as proposições que falam de 'Ciência, Tecnologia e Inovação'?

## Exercício 9: Geração de Imagens com Prompts

Utilizando as informações sumarizadas das proposições dos deputados, vamos gerar prompts que possam fazer alusão aos temas e o que está sendo proposto. Use o Google Colab para gerar imagens com o modelo "CompVis/stable-diffusion-v1-4" para duas proposições de sua escolha. Com essas informações, responda:

1. Descreva o funcionamento dos modelos de imagem, segundo suas arquiteturas, limitações e vantagens:
   - Stable Diffusion
   - DALL-e
   - MidJourney
2. Utilize diferentes técnicas de “Estilo Visual” e “Composição”, além de exemplos com negative prompting, para gerar 3 versões de imagem para cada proposição e avalie as diferenças entre os resultados (as imagens) e os prompts (as proposições).

Assim que terminar, salve seu trabalho em PDF nomeando o arquivo conforme a regra “nome_sobrenome_DR4_AT.PDF” e poste como resposta a este AT.