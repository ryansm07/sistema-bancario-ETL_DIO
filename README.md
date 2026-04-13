# Pipeline ETL com IA Generativa: Marketing Bancário Personalizado

Este projeto foi desenvolvido como parte da **Santander Dev Week 2023**. O objetivo é criar um pipeline ETL (Extração, Transformação e Carga) que consome dados de um banco de dados NoSQL (MongoDB), processa informações de clientes e utiliza a API do Google Gemini para gerar mensagens de investimento personalizadas.

## 🚀 Tecnologias Utilizadas

* **Python 3.10+**
* **Pandas**: Manipulação e transformação de dados.
* **MongoDB Atlas**: Armazenamento de dados em nuvem.
* **Google Gemini API**: Geração de conteúdo com Inteligência Artificial.
* **Tenacity**: Implementação de políticas de retentativa (Resiliência).
* **Python-dotenv**: Gerenciamento de variáveis de ambiente e segurança.

## 📋 Arquitetura do Projeto (ETL)

1.  **Extract (Extração):** O script conecta-se ao MongoDB Atlas e extrai coleções de `users`, `accounts` e `cards`.
2.  **Transform (Transformação):** Utilizando Pandas, os dados são cruzados (merge) e limpos para criar uma visão única de cada cliente.
3.  **Load (Carga & IA):** * O nome de cada cliente é enviado para o modelo **Gemini 1.5 Flash**.
    * A IA gera um conselho de investimento personalizado com limite de 200 caracteres.
    * O resultado é inserido de volta no perfil do usuário no MongoDB na seção `News`.

## ⚙️ Como Executar o Projeto

### 1. Pré-requisitos
* Ter o Python instalado.
* Uma conta no [Google AI Studio](https://aistudio.google.com/) para obter sua API Key.
* Um cluster no [MongoDB Atlas](https://www.mongodb.com/cloud/atlas).

### 2. Instalação
Clone o repositório e instale as dependências:
```bash
git clone [https://github.com/seu-usuario/seu-repositorio.git](https://github.com/seu-usuario/seu-repositorio.git)
cd seu-repositorio
pip install -r requirements.txt
