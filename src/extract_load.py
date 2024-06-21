# import
import yfinance as yf  
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# import das minhas variáveis do ambiente

commodities = ['CL=F', 'GC=F', 'SI=F']

def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'):
    ticker = yf.Ticker('CL=F')
    dados = ticker.history(period = periodo, interval = intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados

def buscar_todos_dados_commodities(commoities):
    todos_dados = []
    for simbolo in commodities:
        dados = buscar_todos_dados_commodities(simbolo)
        todos_dados.append(dados)
    return pd.concat(todos_dados)    
    
  
# pegar a cotação do meus ativos

# concatenar os meus ativos (1..2..3) -> (1)

# salvar no banco de dados