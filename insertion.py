import pandas as pd
from pymongo import MongoClient

def inserer_dataframe(df,db,collection_name):
    if not df.empty:
        df=df.fillna("")
       
        db[collection_name].insert_many(df.to_dict(orient="records"))
        print(f"{len(df.to_dict(orient="records"))} documents insérés dans '{collection_name}'")
    else:
        print(f"dataframe est vide pour {collection_name}")
   
