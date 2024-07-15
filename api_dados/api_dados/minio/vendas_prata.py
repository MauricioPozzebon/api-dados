import s3fs
import pandas as pd
import json
from datetime import datetime

# Inicializando o sistema de arquivos MinIO
fs = s3fs.S3FileSystem(
    key='ROOT',  # Seu MINIO_ROOT_USER
    secret='PASSWORD',  # Sua MINIO_ROOT_PASSWORD
    client_kwargs={'endpoint_url': 'http://localhost:9000'}
)

# Caminho do bucket e pasta na camada BRONZE
bucket_path_bronze = 'dados-mercado/BRONZE/'
bucket_path_prata = 'dados-mercado/PRATA/'

# Listando todos os arquivos JSON na pasta BRONZE
json_files = fs.glob(f'{bucket_path_bronze}*.json')

# Inicializando uma lista para armazenar os DataFrames
dataframes = []

# Lendo cada arquivo JSON, transformando e convertendo para DataFrame
for json_file in json_files:
    with fs.open(json_file, 'rb') as f:
        data = json.load(f)

        # Preparando listas para cada coluna
        rows = []

        # Percorrendo as vendas para preencher as listas
        for venda in data['vendas']:
            timestamp = venda['timestamp']
            cliente_id = venda['cliente']['id'] if venda['cliente']['id'] else 9999
            cliente_nome = venda['cliente']['nome'] if venda['cliente']['nome'] else 'não identificado'

            for item in venda['itens']:
                item_nome = item['item']
                preco = item['preço']

                # Criando uma linha para cada item
                rows.append({
                    'timestamp': timestamp,
                    'id': cliente_id,
                    'nome': cliente_nome,
                    'item': item_nome,
                    'preço': preco
                })

        # Convertendo as linhas para um DataFrame do pandas
        df = pd.DataFrame(rows)
        dataframes.append(df)

# Concatenando todos os DataFrames em um único DataFrame
final_df = pd.concat(dataframes, ignore_index=True)

# Listando e deletando os arquivos Parquet existentes na pasta PRATA
parquet_files = fs.glob(f'{bucket_path_prata}*.parquet')
for parquet_file in parquet_files:
    fs.rm(parquet_file)

# Gerando o timestamp para o nome do novo arquivo Parquet
timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
parquet_filename = f'{bucket_path_prata}vendas_{timestamp}.parquet'

# Salvando o DataFrame final em formato Parquet na camada PRATA do MinIO
with fs.open(parquet_filename, 'wb') as f:
    final_df.to_parquet(f, engine='pyarrow')

print("Arquivo Parquet salvo com sucesso na camada PRATA.")
