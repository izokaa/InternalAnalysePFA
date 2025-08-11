from pymongo import MongoClient
import pandas as pd

def get_db():
    try:
        
        uri="mongodb+srv://Sal123:uyoige58@cluster0.vd6q5oj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        client=MongoClient(uri)   
        client.server_info()
        db=client["BaseMedicale"]
        print("succes")
        return db
    except:
        print("Erreur")
        return None



