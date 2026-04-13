import pandas as pd
import os
import time
from datetime import date
from pymongo import MongoClient
from google import genai
from google.genai import errors as genai_errors
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

# --- CONFIGURAÇÕES INICIAIS ---
load_dotenv()  # Carrega as variáveis do arquivo .env

# Busca as credenciais de ambiente (Segurança: não expõe chaves no código)
gemini_key = os.getenv("key")
mongo_uri = os.getenv("MONGO_URI") # Recomendável colocar a URL do Mongo no .env também

if not gemini_key:
    raise ValueError("Chave de API do Gemini não encontrada no arquivo .env")

# Inicialização do Cliente Gemini
gemini_client = genai.Client(api_key=gemini_key)

# Conexão com o banco de dados MongoDB
mongo_client = MongoClient(mongo_uri)

# --- FUNÇÕES DE APOIO ---

# Decorador de Re-tentativa: Caso a API falhe por excesso de demanda (Erro 503/429), 
# ele tentará 3 vezes com intervalos crescentes (4s a 10s).
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(genai_errors.ClientError)
)
def generate_ai_news(user_name):
    """
    Utiliza o modelo generativo para criar mensagens personalizadas de investimento.
    """
    try:
        prompt = (f"Você é um especialista em marketing bancário. Crie uma mensagem curta "
                  f"para o cliente {user_name} sobre a importância dos investimentos. "
                  f"Máximo de 200 caracteres.")
        
        # Chamada ao modelo Gemini 2.5 Flash (Otimizado para velocidade e custo)
        response = gemini_client.models.generate_content(
            model='gemini-2.5-flash', 
            contents=prompt
        )
        
        # Verifica se a IA retornou texto (evita quebras por filtros de segurança)
        return response.text if response.text else "Invista no seu futuro! Fale com seu gerente."
    
    except Exception as e:
        print(f"Erro ao processar IA para {user_name}: {e}")
        return f"{user_name}, que tal diversificar sua carteira hoje? Consulte-nos!"

# --- PIPELINE PRINCIPAL (ETL) ---

try:
    # 1. EXTRAÇÃO (Extract)
    db = mongo_client["sistema-bancario"]
    
    # Busca dados das coleções no MongoDB e converte para listas
    users = list(db["users"].find())
    accounts = list(db["accounts"].find())
    cards = list(db["cards"].find())
    
    # 2. TRANSFORMAÇÃO (Transform)
    # Converte listas em DataFrames para manipulação com Pandas
    df_users = pd.DataFrame(users)
    df_accounts = pd.DataFrame(accounts)
    df_cards = pd.DataFrame(cards)

    # Renomeia colunas para facilitar o merge e evitar conflitos de nomes
    df_accounts.rename(columns={"_id": "Account", "number": "number_account", "limit": "limit_account"}, inplace=True)
    df_cards.rename(columns={"_id": "Card", "number": "number_card", "limit": "limit_card"}, inplace=True)
    
    # Unifica as tabelas (Merge) com base nas referências de conta e cartão
    df_temp = df_users.merge(df_accounts, on="Account", how="left")
    df_temp = df_temp.merge(df_cards, on="Card", how="left")

    # Limpeza: remove colunas de ID internas que não serão usadas no marketing
    df_temp.drop(columns=["_id", "Account", "Card"], inplace=True, errors="ignore")

    print("Pipeline de dados consolidado com sucesso. Iniciando geração de conteúdo...")

    # 3. CARREGAMENTO (Load)
    # Itera sobre os usuários (limitado ao head(1) para testes/economia de créditos)
    for name in df_temp['name'].head(1):
        print(f"Processando: {name}")
        
        # Gera o conteúdo personalizado via IA
        news_content = generate_ai_news(name)
        
        # Delay de segurança para respeitar os limites da API Gratuita
        time.sleep(2) 

        # Estrutura o documento da nova notícia
        noticia_doc = {
            "icon": "./src/icons/noticia.png", 
            "description": news_content, 
            "date": date.today().isoformat()
        }

        # Insere a notícia gerada de volta no perfil do usuário no MongoDB
        db['users'].update_one(
            {'name': name},
            {"$push": {"News": noticia_doc}}
        )
        print(f"Notícia enviada para o banco: {name}")

except Exception as e:
    # Captura e exibe qualquer erro ocorrido no processo
    print(f"Falha crítica na execução do script: {e}")
    raise