from ConnexionMongo_DB import get_db
from flask import Flask,jsonify,request
from pymongo import MongoClient
from datetime import datetime
import json
import pandas as pd
from DefinitionMethod_s import detecter_changement_par_plateforme
app=Flask(__name__)
client=MongoClient("mongodb+srv://Sal123:uyoige58@cluster0.vd6q5oj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
collection=client["BaseMedicale"]["collection_globale"]
#Calculer le nombre des medecins par ville:
@app.route("/api/kpi",methods=["Get"])
def get_kpi():
    #Récupérer les filtres depuis l'url:
    #plateforme=request.args.get("plateforme")
    #ville=request.args.get("ville")
    #specialite=request.args.get("specialite")
    filtre={}
    #if plateforme:
        #filtre["Plateforme"]=plateforme
    #if ville:
        #filtre["Ville"]=ville
    #if specialite:
        #filtre["Spécialité"]=specialite
    date_extraction=collection.distinct("Date Extraction")
    date_extraction=max(date_extraction)
    #Le nombre total des medecins
    total=collection.count_documents({})
    #Le nombre total des spécialités
    total_spécialités=list(collection.aggregate([
        {"$match":{"Date Extraction":date_extraction}},
        {"$group":{"_id":"$Spécialité.libelle"}},
        {"$count":"nbre_spécialités"}]))
    #Le nombre total des villes
    total_villes=list(collection.aggregate([
        {"$match":{"Date Extraction":date_extraction}},
        {"$group":{"_id":"$Ville.libelle"}},
        {"$count":"nbre_villes"}
    ]))
    #Le nombre des medecins Par Ville
    medecins_par_ville=list(collection.aggregate([
        {"$match":{"Date Extraction":date_extraction}},
        {"$group":{"_id":"$Ville.libelle","count":{"$sum":1}}},
        {"$sort":{"count":-1}}
    ]))
    df_medecins_par_ville=pd.DataFrame(medecins_par_ville)
    #Le nombre des medecins Par Spécialité
    medecins_par_specialite=list(collection.aggregate([
        {"$match":{"Date Extraction":date_extraction}},
        {"$group":{"_id":"$Spécialité.libelle","count":{"$sum":1}}},
        {"$sort":{"count":-1}}
    ]))
    df_medecins_par_specialite=pd.DataFrame(medecins_par_specialite).rename(columns={"_id":"Spécialité","count":"nombre_medecins"})
    #Le nombre des medecins Par Statut
    medecins_par_statut=list(collection.aggregate([
        {"$match":{"Date Extraction":date_extraction}},
        {"$group":{"_id":"$Statut.libelle","count":{"$sum":1}}},
        {"$sort":{"count":-1}}
    ]))
    df_medecins_par_statut=pd.DataFrame(medecins_par_statut).rename(columns={"_id":"Statut","count":"nombre_medecins"})
    #Le nombre des medecins Par Plateforme
    medecins_par_plateforme=list(collection.aggregate([
        {"$match":{"Date Extraction":date_extraction}},
        {"$group":{"_id":"$Plateforme.libelle","count":{"$sum":1}}},
        {"$sort":{"count":-1}}
    ]))
    df_medecins_par_plateforme=pd.DataFrame(medecins_par_plateforme).rename(columns={"_id":"Plateforme","count":"nombre_medecins"})
    #Le nombre des villes couvertes Par plateforme
    ville_par_plateforme=list(collection.aggregate([
        {"$match":{"Date Extraction":date_extraction}},
        {"$group":{"_id":{"Plateforme":"$Plateforme.libelle","Ville":"$Ville.libelle"}}},
        {"$group":{"_id":"$_id.Plateforme","nb_villes":{"$sum":1},
        "Villes": {"$addToSet": "$_id.Ville"}}},
        {"$sort":{"nb_villes":-1}}
    ]))
    df_ville_par_plateforme=pd.DataFrame(ville_par_plateforme).rename(columns={"_id":"Plateforme","count":"NbrVilles"})
    #Le nombre des spécialités par plateforme
    specialite_par_plateforme=list(collection.aggregate([
        {"$match":{"Date Extraction":date_extraction}},
        {"$group":{"_id":{"Plateforme":"$Plateforme.libelle","Spécialité":"$Spécialité.libelle"}}},
        {"$group":{"_id":"$_id.Plateforme","nbre_spécialités":{"$sum":1},
        "Spécialités": {"$addToSet": "$_id.Spécialité"}}},
        {"$sort":{"nbre_spécialités":-1}}
    ]))
    df_specialite_par_plateforme=pd.DataFrame(specialite_par_plateforme).rename(columns={"_id":"Plateforme","count":"NbrSpécialités"})
    #Le nombre des actifs par plateforme
    actifs_par_plateforme=list(collection.aggregate([
        {"$match":{"Date Extraction":date_extraction}},
        {"$match":{"Statut.libelle":"Actif"}},
        {"$group":{"_id":"$Plateforme.libelle","nbre_actifs":{"$sum":1}}},
        #{"$group":{"_id":"$_id.Plateforme","nbre_actifs":{"$sum":1}}},
        {"$sort":{"nbre_actifs":-1}}
    ]))
    df_actifs_par_plateforme=pd.DataFrame(actifs_par_plateforme).rename(columns={"_id":"Plateforme","count":"NbrActifs"})
    #Le nombre des passifs par plateforme
    passifs_par_plateforme=list(collection.aggregate([
        {"$match":{"Date Extraction":date_extraction}},
        {"$match":{"Statut.libelle":"Passif"}},
        {"$group":{"_id":"$Plateforme.libelle","nbre_passifs":{"$sum":1}}},
        {"$sort":{"nbre_passifs":-1}}
    ]))
    df_passifs_par_plateforme=pd.DataFrame(passifs_par_plateforme).rename(columns={"_id":"Plateforme","count":"NbrPassifs"})
    #Le nombre des fictifs par plateforme
    fictifs_par_plateforme=list(collection.aggregate([
        {"$match":{"Date Extraction":date_extraction}},
        {"$match":{"Statut.libelle":"Fictif"}},
        {"$group":{"_id":"$Plateforme.libelle","nbre_fictifs":{"$sum":1}}},
        {"$sort":{"nbre_fictifs":-1}}
    ]))
    df_fictifs_par_plateforme=pd.DataFrame(fictifs_par_plateforme).rename(columns={"_id":"Plateforme","count":"Nbrfictifs"})
    
    df_detecter_presence_absence=detecter_changement_par_plateforme()
    detecter_presence_absence=df_detecter_presence_absence.to_json(orient="records",force_ascii=False)
    with pd.ExcelWriter("visualisation_kpi_excel.xlsx",engine='openpyxl') as writer:
        df_medecins_par_ville.to_excel(writer,sheet_name='medecins_par_ville',index=False)
        df_medecins_par_specialite.to_excel(writer,sheet_name="medecins_par_specialite",index=False)
        df_medecins_par_statut.to_excel(writer,sheet_name="mdecins_par_statut",index=False)
        df_medecins_par_plateforme.to_excel(writer,sheet_name="medecins_par_plateforme",index=False)
        df_ville_par_plateforme.to_excel(writer,sheet_name="ville_par_plateforme",index=False)
        df_specialite_par_plateforme.to_excel(writer,sheet_name="specialite_par_plateforme",index=False)
        df_actifs_par_plateforme.to_excel(writer,sheet_name="actifs_par_plateforme",index=False)
        df_passifs_par_plateforme.to_excel(writer,sheet_name="passifs_par_plateforme",index=False)
        df_fictifs_par_plateforme.to_excel(writer,sheet_name="fictifs_par_plateforme",index=False)
        df_detecter_presence_absence.to_excel(writer,sheet_name="df_detecter_presence_absence",index=False)
    print("total",total),
    print("total_spécialités",total_spécialités)
    print("total_villes",total_villes)
    print("par_specialite",medecins_par_specialite)
    print("par_ville",medecins_par_ville)
    print("par_statut",medecins_par_statut)
    print("par_plateforme",medecins_par_plateforme)
    print("ville_par_plateforme",ville_par_plateforme)
    print("specialites_par_plateforme",specialite_par_plateforme)
    print("actifs_par_plateforme",actifs_par_plateforme)
    print("fictifs_par_plateforme",fictifs_par_plateforme)
    
    return {
        "total":total,
        "total_spécialités":total_spécialités,
        "total_villes":total_villes,
        "par_specialite":medecins_par_specialite,
        "par_ville":medecins_par_ville,
        "par_statut":medecins_par_statut,
        "par_plateforme":medecins_par_plateforme,
        "ville_par_plateforme":ville_par_plateforme,
        "specialites_par_plateforme":specialite_par_plateforme,
        "actifs_par_plateforme":actifs_par_plateforme,
        "passifs_par_plateforme":passifs_par_plateforme,
        "fictifs_par_plateforme":fictifs_par_plateforme
        
    }

result=get_kpi()
with open("kpi_resultats.json", "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)
print(result)
    