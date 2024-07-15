import s3fs
import pandas as pd
from datetime import datetime

# Inicializando o sistema de arquivos MinIO
fs = s3fs.S3FileSystem(
    key='ROOT',  # Seu MINIO_ROOT_USER
    secret='PASSWORD',  # Sua MINIO_ROOT_PASSWORD
    client_kwargs={'endpoint_url': 'http://localhost:9000'}
)

# Caminho dos buckets e pastas
bucket_path_prata = 'dados-mercado/PRATA/'
bucket_path_ouro = 'dados-mercado/OURO/'

# Listando o arquivo Parquet mais recente na pasta PRATA
parquet_files = fs.glob(f'{bucket_path_prata}*.parquet')
if parquet_files:
    latest_parquet_file = max(parquet_files, key=lambda x: x.split('_')[-1].split('.')[0])
    
    # Lendo o arquivo Parquet em um DataFrame
    with fs.open(latest_parquet_file, 'rb') as f:
        df = pd.read_parquet(f)
        
    # Calculando as vendas totais
    vendas_totais = df['preço'].sum()
    
    # Identificando o item mais comprado
    item_mais_comprado = df['item'].mode()[0]
    
    # Determinando qual cliente fez mais compras (em termos de quantidade de itens comprados)
    cliente_mais_compras = df['id'].value_counts().idxmax()
    cliente_mais_compras_nome = df[df['id'] == cliente_mais_compras]['nome'].iloc[0]
    
    # Determinando qual cliente gastou mais (pela soma dos preços das compras)
    cliente_gastos = df.groupby('id')['preço'].sum().idxmax()
    cliente_gastos_valor = df.groupby('id')['preço'].sum().max()
    cliente_gastos_nome = df[df['id'] == cliente_gastos]['nome'].iloc[0]
    
    # Gerando o timestamp atual
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Criando um DataFrame com as novas informações
    new_row = pd.DataFrame({
        'timestamp': [timestamp],
        'vendas_totais': [vendas_totais],
        'item_mais_comprado': [item_mais_comprado],
        'cliente_mais_compras_id': [cliente_mais_compras],
        'cliente_mais_compras_nome': [cliente_mais_compras_nome],
        'cliente_gastos_id': [cliente_gastos],
        'cliente_gastos_nome': [cliente_gastos_nome],
        'cliente_gastos_valor': [cliente_gastos_valor]
    })
    
    # Caminho do arquivo Parquet na camada OURO
    ouro_filename = f'{bucket_path_ouro}vendas_summary.parquet'
    
    # Verificando se já existe uma tabela na camada OURO
    if fs.exists(ouro_filename):
        # Lendo a tabela existente
        with fs.open(ouro_filename, 'rb') as f:
            existing_df = pd.read_parquet(f)
        
        # Concatenando a nova linha à tabela existente
        updated_df = pd.concat([existing_df, new_row], ignore_index=True)
    else:
        # Se não existir, a nova tabela será a linha atual
        updated_df = new_row
    
    # Salvando a tabela atualizada na camada OURO
    with fs.open(ouro_filename, 'wb') as f:
        updated_df.to_parquet(f, engine='pyarrow')

    print("Tabela atualizada na camada OURO:")
    print(updated_df)
else:
    print("Nenhum arquivo Parquet encontrado na camada PRATA.")
