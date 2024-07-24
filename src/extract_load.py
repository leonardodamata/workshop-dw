# import
import yfinance as yf  
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from dotenv import load_dotenv
import os

# Carregar variáveis do arquivo .env
load_dotenv()

# Obter as variáveis do arquivo .env
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

# Verifique se todas as variáveis necessárias estão definidas
if not all([DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME]):
    missing_vars = [var for var, value in zip(
        ['DB_USER_PROD', 'DB_PASS_PROD', 'DB_HOST_PROD', 'DB_PORT_PROD', 'DB_NAME_PROD'],
        [DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME]
    ) if not value]
    raise ValueError(f"Um ou mais valores de configuração do banco de dados estão faltando: {', '.join(missing_vars)}")

# Construir o URL de conexão
db_config = {
    'drivername': 'postgresql',
    'username': DB_USER,
    'password': DB_PASS,
    'host': DB_HOST,
    'port': int(DB_PORT),  # Converta a porta para inteiro para evitar o erro
    'database': DB_NAME
}

# Verifique o valor da porta antes de continuar
print(f"Conectando ao banco de dados na porta: {db_config['port']}")

# Crie o URL do banco de dados e o engine
try:
    database_url = URL.create(**db_config)
    engine = create_engine(database_url)
    print("Conexão com o banco de dados estabelecida com sucesso.")
except Exception as e:
    print(f"Erro ao conectar ao banco de dados: {e}")

# import das minhas variáveis do ambiente
 

commodities = ['CL=F', 'GC=F', 'SI=F']

def buscar_dados_commodities(simbolo, periodo='5y', intervalo='1d'):
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period = periodo, interval = intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados

def buscar_todos_dados_commodities(commodities):
    todos_dados = []
    for simbolo in commodities:
        dados = buscar_dados_commodities(simbolo)
        todos_dados.append(dados)   
    return pd.concat(todos_dados)    

def salvar_no_postgres(df, schema='public'):
    df.to_sql('commodities', engine, if_exists='replace', index='True', index_label='Date', schema=schema)
    
    
if __name__ == "__main__"  :
    dados_concatenados = buscar_todos_dados_commodities(commodities)
    print(dados_concatenados)
    salvar_no_postgres(dados_concatenados, schema='public')
# pegar a cotação do meus ativos

# concatenar os meus ativos (1..2..3) -> (1)

# salvar no banco de dados