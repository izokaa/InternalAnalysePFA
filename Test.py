from DefinitionMethod_s import unifier_dataframe,attribuer_ids_df_hash_simple,lancer_scrapping_med,lancer_scrapping_nabady,lancer_scrapping_dabadoc,nettoyer_dataframe_medecins,detecter_changement_par_plateforme,lancer_scrapping_docdialy
import pandas as pd 
from pymongo import MongoClient
from insertion import inserer_dataframe
from ConnexionMongo_DB import get_db
from datetime import datetime
date_extraction=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#-----------------------Lancer Scrapping + Nettoyage-----------------------------
df1=lancer_scrapping_dabadoc()
df1["Date Extraction"]=date_extraction
#df1
df2=lancer_scrapping_med()
df2["Date Extraction"]=date_extraction
#df2
df3=lancer_scrapping_nabady()
df3["Date Extraction"]=date_extraction
#df3
#df4=lancer_scrapping_docdialy()
df4=pd.read_excel(r"C:\Users\HP\Desktop\Nouveau dossier (2)\Fichier_DocDialy_ma.xlsx")
df4=nettoyer_dataframe_medecins(df4,"Docdialy")
df4["Date Extraction"]=date_extraction
#df4
#-----------------Unifier le shéma----------------------------
colonnes_standard=["ID Medecin","Statut","Prénom","Nom","Sexe","Spécialité","Ville","Date de Naissance","Téléphone","WhatsApp","Fix","Email","Duree","Date de Connexion","Date Extraction","Plateforme"]
df1=unifier_dataframe(df1,"Dabadoc")
df2=unifier_dataframe(df2,"Med")
df3=unifier_dataframe(df3,"Nabady")
df4=unifier_dataframe(df4,"Docdialy")
df_final=pd.concat([df1,df2,df3,df4],ignore_index=True)
#------------------Associer des identifiants à la spécialité----------------------
db = get_db()
df_final, id_maps = attribuer_ids_df_hash_simple(df_final, db, collection_name="collection_globale")
#df_final.to_excel(r"C:\Users\HP\Desktop\Data_Folder\df_Version.xlsx",index=False)
####Enregistrer dans MongoDB:
inserer_dataframe(df_final,get_db(),"collection_globale")
####Detecter les elements présents,absents et modifiés:
detecter_changement_par_plateforme()
