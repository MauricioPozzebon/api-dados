import random
import uuid
from collections import Counter
from datetime import datetime
from random import choice

import pytz
from faker import Faker
from fastapi import FastAPI, Path

app = FastAPI(title='Gerador de Dados (faker)',
description='''API que gera dados fictícios de vendas, clientes e produtos para 
um "mercadinho de bairro" utilizando o pacote **faker**.

O *endpoint* **/vendas** deve ser usado observando o seguinte cenário de negócio:
- Número de vendas a cada 5 minutos: 1-5
- Número de itens em cada cesta de compras: 1-8
- Valor máximo da cesta: R$200
- Preço de cada item: R$0,99-R$150
- Clientes registrados: 200
- Compras de clientes registrados: 40%
- Ítens registrados: 1200
- Categorias : Açougue, Frios e laticínios, Adega e bebidas, Higiene e limpeza, Hortifruti e mercearia, Padaria, Enlatados, Cereais

A *seed* fixa a base de clientes, itens e preços; a cesta é gerada de forma aleatória.

Os *endpoints* **/clientes** e **/itens** geram uma base aleatória de clientes + id e itens + preço + categoria + estoque. 



Veja o código no [GitHub](https://github.com/MauricioPozzebon/api-dados), meu [portfólio](https://mauricio-pozzebon.netlify.app/)
e [linkedin](https://www.linkedin.com/in/mauriciopozzebon/).
''')


@app.get('/vendas/{seed}')
def vendas(seed: int = Path(..., description="Semente aleatória")):
    # Definindo a seed para garantir resultados replicáveis
    SEED = seed

    # Inicializando o gerador de Faker com a localidade pt_BR e definindo a seed
    fake = Faker('pt_BR')
    Faker.seed(SEED)

    random.seed(SEED)

    # Lista para armazenar os clientes
    clientes = []

    # Conjunto para rastrear ids únicos
    ids_usados = set()

    # Gerar id único de até 4 dígitos
    def gerar_id_unico():
        while True:
            id_aleatorio = random.randint(100, 9999)
            if id_aleatorio not in ids_usados:
                ids_usados.add(id_aleatorio)
                return id_aleatorio

    # Gerar 200 clientes
    for _ in range(200):
        cliente = {
            "id": gerar_id_unico(),
            "nome": fake.name()
        }
        clientes.append(cliente)

    # Filtrar clientes para garantir unicidade dos nomes
    nomes_vistos = set()
    clientes_unicos = []

    for cliente in clientes:
        if cliente["nome"] not in nomes_vistos:
            nomes_vistos.add(cliente["nome"])
            clientes_unicos.append(cliente)

    # Lista de categorias
    categorias = [
        "Açougue",
        "Frios e laticínios",
        "Adega e bebidas",
        "Higiene e limpeza",
        "Hortifruti e mercearia",
        "Padaria",
        "Enlatados",
        "Cereais"
    ]

    # Gerando 1200 produtos únicos de duas palavras
    prods = []
    for _ in range(1200):
        prod = fake.words(nb=2)
        # Juntando as duas palavras em uma única string
        prod_combined = ' '.join(prod)
        prods.append(prod_combined)

    # Gerando 1200 valores únicos no intervalo de 0,99 a 150 reais
    prices = set()  # Usamos um set para garantir a unicidade

    while len(prices) < 1200:
        # Gerando um preço aleatório entre 1 e 150 reais com duas casas decimais
        price = fake.pyfloat(left_digits=3, right_digits=2, positive=True, min_value=0.99, max_value=150)
        prices.add(price)

    # Convertendo o set para uma lista e embaralhando
    prices = list(prices)
    random.shuffle(prices)

    # Emparelhando os produtos com seus valores únicos e categorias aleatórias
    product_values = [{
        "item": prods[i], 
        "preço": prices[i], 
        "categoria": random.choice(categorias)
    } for i in range(len(prods))]

    # Criar uma nova lista com produtos únicos
    produtos_vistos = set()
    produtos_unicos = []

    for produto in product_values:
        if produto["item"] not in produtos_vistos:
            produtos_vistos.add(produto["item"])
            produtos_unicos.append(produto)

    random.seed()

    # Função para gerar uma compra com o valor total entre 3 e 200 reais
    def gerar_compra_valida():
        while True:
            # Selecionando um número aleatório de produtos para a compra (entre 1 e 8)
            num_produtos = random.randint(1, 8)
            # Selecionando produtos únicos aleatoriamente para a compra
            produtos_comprados = random.sample(produtos_unicos, num_produtos)
            # Calculando o valor total da compra
            total_valor = sum(produto['preço'] for produto in produtos_comprados)
            # Verificando se o valor total está entre 0.99 e 200 reais
            if 0.99 <= total_valor <= 200:
                return produtos_comprados

    # Gerando entre 1 e 5 compras
    num_compras = random.randint(1, 5)
    compras = []
    for _ in range(num_compras):
        compra = gerar_compra_valida()
        # Criando uma lista de dicionários para a compra
        compra_formatada = [{'item': prod['item'], 'preço': float(f'{prod["preço"]:.2f}'), 'categoria': prod['categoria']} for prod in compra]
        # Obtendo o horário atual
        timezone_brasilia = pytz.timezone('America/Sao_Paulo')
        horario_atual_brasilia = datetime.now(timezone_brasilia).isoformat()
        # Cliente como None até a atribuição
        compras.append({'timestamp': horario_atual_brasilia, 'itens': compra_formatada, 'cliente': None})

    # Adicionando clientes registrados em 40% das compras
    num_compras_com_cliente = int(0.4 * len(compras))

    # Selecionar aleatoriamente compras para ter clientes
    compras_com_cliente_indices = random.sample(range(len(compras)), num_compras_com_cliente)

    for indice in compras_com_cliente_indices:
        # Selecionar aleatoriamente um cliente da lista de clientes
        cliente_aleatorio = random.choice(clientes_unicos)
        # Adicionar o cliente à compra
        compras[indice]['cliente'] = cliente_aleatorio

    # Para compras sem cliente, definir 'Cliente' como {'id': None, 'nome': None}
    for compra in compras:
        if compra['cliente'] is None:
            compra['cliente'] = {'id': None, 'nome': None}

    formas_pagamento = ['PIX', 'dinheiro', 'crédito', 'débito']

    for compra in compras:
        forma_pagamento = random.choice(formas_pagamento)
        compra['meio'] = forma_pagamento

    for compra in compras:
        transaction_id = str(uuid.uuid4())
        compra['transaction_id'] = transaction_id

    return {"vendas": compras}


@app.get('/clientes/{seed}/{quantidade}')
def clientes(seed: int = Path(..., description="Semente aleatória"),
    quantidade: int = Path(..., description="Número de clientes a serem gerados")):

    SEED = seed

    # Inicializando Faker com localidade pt_BR
    fake = Faker('pt_BR')
    Faker.seed(SEED)

    random.seed(SEED)

    # Lista para armazenar os clientes
    clientes = []

    # Conjunto para rastrear IDs únicos
    ids_usados = set()

    # Função para gerar id único de até 4 dígitos
    def gerar_id_unico():
        while True:
            id_aleatorio = random.randint(100, 9999)
            if id_aleatorio not in ids_usados:
                ids_usados.add(id_aleatorio)
                return id_aleatorio

    # Gerar n clientes
    for _ in range(quantidade):
        cliente = {
            "id": gerar_id_unico(),
            "nome": fake.name()
        }
        clientes.append(cliente)

    # Contar a ocorrência dos nomes
    nomes = [cliente["nome"] for cliente in clientes]
    nome_counter = Counter(nomes)

    # Deletar clientes com nomes repetidos
    clientes_unicos = []
    nomes_vistos = set()

    for cliente in clientes:
        if cliente["nome"] not in nomes_vistos:
            clientes_unicos.append(cliente)
            nomes_vistos.add(cliente["nome"])

    return {"clientes": clientes_unicos}


@app.get('/itens/{seed}/{quantidade}')
def itens(seed: int = Path(..., description="Semente aleatória"),
          quantidade: int = Path(..., description="Número de itens a serem gerados")):
    # Definindo a seed para garantir resultados replicáveis
    SEED = seed

    # Inicializando o gerador de Faker com a localidade pt_BR
    fake = Faker('pt_BR')
    Faker.seed(SEED)

    random.seed(SEED)

    # Lista de categorias
    categorias = [
        "Açougue",
        "Frios e laticínios",
        "Adega e bebidas",
        "Higiene e limpeza",
        "Hortifruti e mercearia",
        "Padaria",
        "Enlatados",
        "Cereais"
    ]

    # Gerando n produtos únicos de duas palavras
    prods = []
    for _ in range(quantidade):
        prod = fake.words(nb=2)
        # Juntando as duas palavras em uma única string
        prod_combined = ' '.join(prod)
        prods.append(prod_combined)

    # Set para garantir a unicidade
    prices = set()

    # Gerando valores únicos para o preço
    while len(prices) < quantidade:
        # Gerando um preço aleatório entre 0,99 e 150 reais com duas casas decimais
        price = fake.pyfloat(left_digits=3, right_digits=2, positive=True, min_value=0.99, max_value=150)
        prices.add(price)

    # Convertendo o set para uma lista e embaralhando
    prices = list(prices)
    random.shuffle(prices)

    # Emparelhando os produtos com seus valores únicos, categorias e estoque aleatórios
    product_values = [{
        "item": prods[i], 
        "preço": prices[i], 
        "categoria": random.choice(categorias),
        "estoque": random.randint(1, 50)  # Gerando valor de estoque entre 1 e 50
    } for i in range(len(prods))]

    # Contar a ocorrência dos nomes dos produtos
    produtos = [produto["item"] for produto in product_values]

    # Criar uma nova lista com produtos únicos (primeira ocorrência)
    produtos_vistos = set()
    produtos_unicos = []

    for produto in product_values:
        if produto["item"] not in produtos_vistos:
            produtos_vistos.add(produto["item"])
            produtos_unicos.append(produto)

    return {"itens": produtos_unicos}
