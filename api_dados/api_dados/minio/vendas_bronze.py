import requests
import pandas as pd
from io import BytesIO
import s3fs
from datetime import datetime
import json

# URL do endpoint
url = "https://apidadosapp.fly.dev/get/vendas/1"

# Fazendo a requisição GET
response = requests.get(url)

# Verificando se a requisição foi bem-sucedida
if response.status_code == 200:
    # Obtendo os dados JSON da resposta
    data = response.json()

    # Gerando o timestamp para o nome do arquivo
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    json_filename = f'dados-mercado/BRONZE/vendas_{timestamp}.json'

    # Inicializando o sistema de arquivos MinIO
    fs = s3fs.S3FileSystem(
        key='ROOT',  # Seu MINIO_ROOT_USER
        secret='PASSWORD',  # Sua MINIO_ROOT_PASSWORD
        client_kwargs={'endpoint_url': 'http://localhost:9000'}
    )

    # Salvando o arquivo JSON bruto no MinIO
    with fs.open(json_filename, 'wb') as f:
        f.write(json.dumps(data).encode('utf-8'))
