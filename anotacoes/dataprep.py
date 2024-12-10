'''

Implemente em dataprep.py uma função que faça a coleta das informações dos deputados atuais da câmara dos deputados:

1. Colete e salve os dados dos deputados atuais da câmara no arquivo data/deputados.parquet através da url: url_base+/deputados.
2. Execute um prompt para criar o código que gere um gráfico de pizza com o total e o percentual de deputados de cada partido, salvo em 'docs/distribuicao_deputados.png'.
3. Execute um prompt utilizando os resultados da análise anterior (distribuição de deputados por partido) para gerar insights sobre a distribuição de partidos e como isso influencia a câmara. Utilize os elementos de prompts dados, persona e exemplos para instruir o LLM. Explique o objetivo de cada elemento, avalie a resposta e salve-a em data/insights_distribuicao_deputados.json.

'''

import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

url_base = 'https://dadosabertos.camara.leg.br/api/v2'
url_deputados = f'{url_base}/deputados'

response = requests.get(url_deputados)

if response.status_code == 200:
    print("Requisição bem-sucedida")
else:
    print("Erro na requisição:", response.status_code)
print(response.json())  # Imprime o dicionário retornado pela API

deputados = response.json()['dados']  # Acessa a chave 'dados'

df = pd.DataFrame(deputados)

df.to_parquet('data/deputados.parquet')

df = pd.read_parquet('data/deputados.parquet')

# grafico de pizza
df['siglaPartido'].value_counts().plot.pie(
    title='Distribuicao de deputados por partido',
    autopct=lambda p : '{:,.0f} ({:.1f}%)'.format(p * sum(df['siglaPartido'].value_counts()) / 100, p),
    pctdistance=0.8,
    labeldistance=0.5,
    figsize=(10, 10),
    shadow=True,
    startangle=90,
    textprops=dict(color='white')
)

plt.savefig('docs/distribuicao_deputados.png')

# grafico de barras
df['siglaPartido'].value_counts().plot.bar(
    title='Distribuicao de deputados por partido',
    figsize=(10, 6),
    color='#666666',
    edgecolor='white'
)

plt.ylabel('Número de deputados')
plt.xlabel('Partido')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('docs/distribuicao_deputados_barras.png')

df['siglaPartido'].value_counts().to_json('data/insights_distribuicao_deputados.json')