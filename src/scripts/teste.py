from pymongo import MongoClient

client = MongoClient("mongodb+srv://ryanmartins07_adm:cttmpqb3@sistema-bancario.s4nqqwx.mongodb.net/?appName=sistema-bancario")
db = client["teste_debug"]
collection = db["teste"]

collection.insert_one({"msg": "funcionando"})

print("Inserido!")