import pandas as pd #para manipulação dos dados
import os #manipulação de diretorios operacionais
import glob #ler todos os arquivos globalmente em massa
from pymongo import MongoClient #extracao de dados em Python no MongoDB
#from google import genai #geracao de mensagens para as noticias
#from google.genai import types 
from datetime import date #incluir a data que foi enviada a noticia
from key_gemini import key

# Conexão com o MongoDB
mongo_client = MongoClient("mongodb+srv://ryanmartins07_adm:cttmpqb3@sistema-bancario.s4nqqwx.mongodb.net/?appName=sistema-bancario")

# Configuração do Cliente Gemini 
#gemini_client = genai.Client(api_key=key)

# Geração de uma mensagem para cada cliente do banco
#def generate_ai_news(user):
    
    #response = gemini_client.models.generate_content(
    #    model='gemini-2.5-flash', contents= f"Você é um especialista em marketing bancário e preciso que crie uma mensagem para o 
    # usuário {user} sobre a importância dos investimentos com no máximo 200 caracteres"
    #)
    #return response.text


#Retornar erro quando não executa
try:
    #Puxa a base dos dados
    db = mongo_client["sistema-bancario"]

    #Extração de cada coleção da base de dados 
    users = list(db["users"].find())
    accounts = list(db["accounts"].find())
    #features = list(db["features"].find())
    cards = list(db["cards"].find())
    news = list(db["news"].find())
    
    #Transformação de cada tabela em um formato de tabela visual
    df_temp_users = pd.DataFrame(users)
    df_temp_accounts = pd.DataFrame(accounts)
    df_temp_cards = pd.DataFrame(cards)

    #Renomear as colunas
    df_temp_accounts.rename(columns={"_id": "Account"}, inplace=True)
    df_temp_cards.rename(columns={"_id": "Card"}, inplace=True)
    df_temp_cards.rename(columns={"number": "number_card"}, inplace=True)
    df_temp_accounts.rename(columns={"number": "number_account"}, inplace=True)
    df_temp_cards.rename(columns={"limit": "limit_card"}, inplace=True)
    df_temp_accounts.rename(columns={"limit": "limit_account"}, inplace=True)
    
    #Agrupar as colecoes com base nas informações de id de conta e cartão
    df_temp = df_temp_users.merge(df_temp_accounts, on="Account", how="left")
    df_temp = df_temp.merge(df_temp_cards, on="Card", how="left")

    #Exclusao das colunas de referencia
    df_temp.drop(columns=["_id"], inplace=True, errors="ignore")
    df_temp.drop(columns=["Account"], inplace=True, errors="ignore")
    df_temp.drop(columns=["Card"], inplace=True, errors="ignore")

    #Revisao da tabela criada 
    print (df_temp)

    for user in df_temp['name']:
        #news = generate_ai_news(user) #geracao de textos
        #time.sleep(2) #aumento do intervalo de tempo para diminuir os custo da IA generativa versao gratuita
        news = f"{user}, seu futuro começa hoje! Invista para multiplicar seu dinheiro, superar a inflação e realizar seus sonhos. Construa um amanhã mais seguro e próspero. Descubra como!"
        print(news)
        document = {"icon": "./src/icons/noticia.png", "description": news, "date": date.today().isoformat()}
        #mongo_client['sistema-bancario']['users'].update_one({'name': user},{"$push": {"News": document}}) #comentar para não inserir várias mensagens ao rodar o código


except Exception as e:
    raise Exception("Unable to find the document due to the following error: ", e)