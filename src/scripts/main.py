import pandas as pd #para manipulação dos dados
import os #manipulação de diretorios operacionais
import glob #ler todos os arquivos globalmente em massa
from pymongo import MongoClient #extracao de dados em Python no MongoDB

#Conexão com a base de dados do MongoDB
client = MongoClient("mongodb+srv://ryanmartins07_adm:cttmpqb3@sistema-bancario.s4nqqwx.mongodb.net/?appName=sistema-bancario")




#Retornar erro quando não executa
try:
    #Renomeia a base de dados
    db = client["sistema-bancario"]

    #Extração de cada tabela da minha base de dados Sistema Bancario
    users = list(db["users"].find())
    accounts = list(db["accounts"].find())
    features = list(db["features"].find())
    cards = list(db["cards"].find())
    news = list(db["news"].find())
    
    #Transformação de cada tabela em um formato de tabela visual
    df_temp_users = pd.DataFrame(users)
    df_temp_accounts = pd.DataFrame(accounts)
    df_temp_cards = pd.DataFrame(cards)

    df_temp_accounts.rename(columns={"_id": "Account"}, inplace=True)
    df_temp_cards.rename(columns={"_id": "Card"}, inplace=True)
    df_temp_cards.rename(columns={"number": "number_card"}, inplace=True)
    df_temp_accounts.rename(columns={"number": "number_account"}, inplace=True)
    df_temp_cards.rename(columns={"limit": "limit_card"}, inplace=True)
    df_temp_accounts.rename(columns={"limit": "limit_account"}, inplace=True)
    

    df_temp = df_temp_users.merge(df_temp_accounts, on="Account", how="left")
    df_temp = df_temp.merge(df_temp_cards, on="Card", how="left")


    df_temp.drop(columns=["_id"], inplace=True, errors="ignore")
    df_temp.drop(columns=["Account"], inplace=True, errors="ignore")
    df_temp.drop(columns=["Card"], inplace=True, errors="ignore")

    print (df_temp)


except Exception as e:
    raise Exception("Unable to find the document due to the following error: ", e)
