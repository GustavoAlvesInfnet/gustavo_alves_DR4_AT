'''

Implemente em dataprep.py uma função que faça a coleta das informações dos deputados atuais da câmara dos deputados:

1. Colete e salve os dados dos deputados atuais da câmara no arquivo data/deputados.parquet através da url: url_base+/deputados.
2. Execute um prompt para criar o código que gere um gráfico de pizza com o total e o percentual de deputados de cada partido, salvo em 'docs/distribuicao_deputados.png'.
3. Execute um prompt utilizando os resultados da análise anterior (distribuição de deputados por partido) para gerar insights sobre a distribuição de partidos e como isso influencia a câmara. Utilize os elementos de prompts dados, persona e exemplos para instruir o LLM. Explique o objetivo de cada elemento, avalie a resposta e salve-a em data/insights_distribuicao_deputados.json.

'''

import requests
import pandas as pd
import matplotlib.pyplot as plt
import os

# URL base da API
url_base = 'https://dadosabertos.camara.leg.br/api/v2'

# Pasta para salvar os dados
data_path = 'data'
docs_path = 'docs'
os.makedirs(data_path, exist_ok=True)
os.makedirs(docs_path, exist_ok=True)

# Função para coletar dados dos deputados
def get_deputados():
    url = f'{url_base}/deputados'
    response = requests.get(url, headers={"Accept": "application/json"})
    
    if response.status_code == 200:
        # Extrai os dados em JSON
        data = response.json()
        deputados = data['dados']
        return deputados
    else:
        raise Exception(f'Erro ao acessar a API: {response.status_code}')

# Coletar dados dos deputados e salvar em Parquet
def salvar_deputados_parquet(deputados, file_path):
    # Transformar os dados em um DataFrame do pandas
    df = pd.DataFrame(deputados)
    # Salvar no formato Parquet
    df.to_parquet(file_path, index=False)
    print(f'Dados salvos em {file_path}')

# Gerar gráfico de pizza da distribuição de deputados por partido
def gerar_grafico_distribuicao(file_path, output_path):
    # Carregar os dados do arquivo Parquet
    df = pd.read_parquet(file_path)
    
    # Contar o número de deputados por partido
    partido_counts = df['siglaPartido'].value_counts()
    
    # Configurar o gráfico de pizza
    plt.figure(figsize=(10, 8))
    plt.pie(partido_counts, labels=partido_counts.index, autopct='%1.1f%%', startangle=140)
    plt.title('Distribuição de Deputados por Partido')
    plt.axis('equal')  # Garantir que o gráfico seja um círculo
    
    # Salvar o gráfico em um arquivo PNG
    plt.savefig(output_path, format='png')
    print(f'Gráfico salvo em {output_path}')
    plt.close()

# Função para coletar as despesas dos deputados
def get_despesas_deputados(deputados):
    if os.path.exists(os.path.join(data_path, 'despesas_brutas.parquet')):
        # le o arquivo
        despesas = pd.read_parquet(os.path.join(data_path, 'despesas_brutas.parquet'))
    else:
        # Lista para armazenar as despesas dos deputados
        despesas = []
        # Iterar sobre cada deputado
        for deputado in deputados:
            # Obter o ID do deputado
            id_deputado = deputado['id']

            # Obter as despesas do deputado
            url = f'{url_base}/deputados/{id_deputado}/despesas'
            response = requests.get(url, headers={"Accept": "application/json"})
            
            if response.status_code == 200:
                
                # Extrai os dados em JSON
                data = response.json()
                despesas_deputado = data['dados']
    
                
                # Iterar sobre cada despesa do deputado
                for despesa in despesas_deputado:
                    # Adicionar a despesa à lista
                    despesa['idDeputado'] = id_deputado
                    despesas.append(despesa)
                    
            else:
                raise Exception(f'Erro ao acessar a API: {response.status_code}')
        
        parquet_file = os.path.join(data_path, 'despesas_brutas.parquet')
        df = pd.DataFrame(despesas)
        df.to_parquet(parquet_file, index=False)
        print(f'Dados salvos em {parquet_file}')




    # Criar um DataFrame com as despesas
    df = pd.DataFrame(despesas)

    # Verificar se os tipos de dado são corretos
    if 'urlDocumento' in df.columns:
        df['urlDocumento'] = df['urlDocumento'].astype(str)
    
    # Data referência: início em 01/08/2024 e fim em 30/08/2024 (datas inclusivas).
    df = df[(df['dataDocumento'] >= '2024-08-01') & (df['dataDocumento'] <= '2024-08-30')]
    # Agrupar as despesas por dia, deputado e tipo de despesa
    df_agrupado = df.groupby(['dataDocumento', 'idDeputado', 'tipoDespesa']).sum().reset_index()

    # Salvar o DataFrame no formato Parquet
    parquet_file = os.path.join(data_path, 'serie_despesas_diarias_deputados.parquet')
    df_agrupado.to_parquet(parquet_file, index=False)
    print(f'Dados salvos em {parquet_file}')

    #salva como json tbm
    json_file = os.path.join(data_path, 'serie_despesas_diarias_deputados.json')
    df_agrupado.to_json(json_file, orient='records')
    print(f'Dados salvos em {json_file}')

if __name__ == "__main__":
    # Caminhos para salvar os arquivos
    parquet_file = os.path.join(data_path, 'deputados.parquet')
    grafico_file = os.path.join(docs_path, 'distribuicao_deputados.png')
    '''
    # Coletar e salvar os dados dos deputados
    try:
        deputados = get_deputados()
        salvar_deputados_parquet(deputados, parquet_file)
        
    except Exception as e:
        print(f'Erro ao coletar e salvar os dados: {e}')
    '''
    try:
        deputados = pd.read_parquet(parquet_file)
        deputados = deputados.to_dict('records')
        get_despesas_deputados(deputados)
    except Exception as e:
        print(f'Erro ao coletar as despesas dos deputados: {e}')
    
    '''
    # Gerar o gráfico de pizza
    try:
        gerar_grafico_distribuicao(parquet_file, grafico_file)
    except Exception as e:
        print(f'Erro ao gerar o gráfico: {e}')
    '''