from ConnexionMongo_DB import get_db
from flask import Flask, jsonify, request
from pymongo import MongoClient
from datetime import datetime
import json
import pandas as pd
from ConnexionMongo_DB import get_db
from DefinitionMethod_s import detecter_changement_par_plateforme
# --- Connexion / Collection (ajuste selon ce que retourne get_db) ---
uri="mongodb+srv://Sal123:AZEQSDAZEQSD@cluster0.vd6q5oj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client=MongoClient(uri)
collection = client["BaseMedicale"]["collection_globale"]

# ----------------------------- ROUTE UNIQUE -----------------------------
#plateforme_id = request.args.get("plateforme_id")
#ville_id      = request.args.get("ville_id")
#specialite_id = request.args.get("specialite_id")
#statut        = request.args.get("statut")   # "Actif" / "Passif" / "Fictif"


def get_kpi():
    
    # --- Dates à comparer (2 dernières) ---
    dates = sorted(collection.distinct("Date Extraction"))
    if len(dates) < 2:
        return "error Pas assez de versions", 400
    date_new = max(dates)

    # --- Métriques de base (à la dernière extraction) ---
    date_extraction = date_new
    match = {"Date Extraction": date_extraction}
    #if plateforme_id:
    #    match["Plateforme.id"] = plateforme_id
    #if ville_id:
    #    match["Ville.id"] = ville_id
    #if specialite_id:
    #   match["Spécialité.id"] = specialite_id
    #if statut:
    #   match["$or"] = [ {"Statut.libelle": statut}, {"Statut": statut} ]
    total = collection.count_documents({"Date Extraction": date_extraction})

    total_spécialités = list(collection.aggregate([
        {"$match": {"Date Extraction": date_extraction}},
        {"$group": {"_id": "$Spécialité.id"}},
        {"$count": "nbre_spécialités"}
    ]))
    
    total_villes = list(collection.aggregate([
        {"$match": {"Date Extraction": date_extraction}},
        {"$group": {"_id": "$Ville.id"}},
        {"$count": "nbre_villes"}
    ]))

    medecins_par_ville = list(collection.aggregate([
        #{"$match": match},
        {"$match": {"Date Extraction": date_extraction}},
        {"$group": {"_id": "$Ville.id","libelle": {"$first": "$Ville.libelle"}, "nbre_medecins_villes": {"$sum": 1}}}, 
        {"$sort": {"nbre_medecins_villes": -1}}
    ]))
    df_medecins_par_ville = pd.DataFrame(medecins_par_ville)

    medecins_par_specialite = list(collection.aggregate([
        #{"$match": match},
        {"$match": {"Date Extraction": date_extraction}},
        {"$group": {"_id": "$Spécialité.id","libelle": {"$first": "$Ville.libelle"}, "nbre_medecins_specialites": {"$sum": 1}}},
        {"$sort": {"nbre_medecins_specialites": -1}}
    ]))
    df_medecins_par_specialite = pd.DataFrame(medecins_par_specialite)

    medecins_par_statut = list(collection.aggregate([
        #{"$match": match},
        {"$match": {"Date Extraction": date_extraction}},
        {"$group": {"_id": "$Statut.id","libelle": {"$first": "$Statut.libelle"},"nbre_medecins_statut": {"$sum": 1}}},
        {"$sort": {"nbre_medecins_statut": -1}}
    ]))
    df_medecins_par_statut = pd.DataFrame(medecins_par_statut)

    medecins_par_plateforme = list(collection.aggregate([
        #{"$match": match},
         {"$match": {"Date Extraction": date_extraction}},
        {"$group": {"_id": "$Plateforme.id","libelle": {"$first":"$Plateforme.libelle"},"nbre_medecins_plateforme": {"$sum": 1}}},
        {"$sort": {"nbre_medecins_plateforme": -1}}
    ]))
    df_medecins_par_plateforme = pd.DataFrame(medecins_par_plateforme)

    ville_par_plateforme = list(collection.aggregate([
        #{"$match": match},
         {"$match": {"Date Extraction": date_extraction}},
        {"$group": {"_id": {"plateforme_id": "$Plateforme.id","plateforme_libelle":"$Plateforme.libelle", "Ville": "$Ville.libelle"}}},
        {"$group": {"_id": "$_id.plateforme_id","libelle": {"$first":"$_id.plateforme_libelle"}, "nbre_villes_plateforme": {"$sum": 1},
                    "Villes": {"$addToSet": "$_id.Ville"}}},
        {"$sort": {"nbre_villes_plateforme": -1}}
    ]))
    df_ville_par_plateforme = pd.DataFrame(ville_par_plateforme)

    specialite_par_plateforme = list(collection.aggregate([
        #{"$match": match},
        {"$match": {"Date Extraction": date_extraction}},
        {"$group": {"_id": {"plateforme_id": "$Plateforme.id","plateforme_libelle":"$Plateforme.libelle", "Spécialité": "$Spécialité.libelle"}}},
        {"$group": {"_id": "$_id.plateforme_id","libelle": {"$first":"$_id.plateforme_libelle"}, "nbre_spécialités_plateforme": {"$sum": 1},
                    "Spécialités": {"$addToSet": "$_id.Spécialité"}}},
        {"$sort": {"nbre_spécialités_plateforme": -1}}
    ]))
    df_specialite_par_plateforme = pd.DataFrame(specialite_par_plateforme)

    actifs_par_plateforme = list(collection.aggregate([
         #{"$match": match},
        {"$match": {"Date Extraction": date_extraction}},
        {"$match": {"Statut.libelle": "Actif"}},
        {"$group": {"_id": "$Plateforme.id", "libelle": {"$first":"$Plateforme.libelle"},"nbre_actifs_plateforme": {"$sum": 1}}},
        {"$sort": {"actifs_plateforme": -1}}
    ]))
    df_actifs_par_plateforme = pd.DataFrame(actifs_par_plateforme)

    passifs_par_plateforme = list(collection.aggregate([
        #{"$match": match},
        {"$match": {"Date Extraction": date_extraction}},
        {"$match": {"Statut.libelle": "Passif"}},
        {"$group": {"_id": "$Plateforme.id","libelle": {"$first":"$Plateforme.libelle"},"nbre_passifs_plateforme": {"$sum": 1}}},
        {"$sort": {"nbre_passifs_plateforme": -1}}
    ]))
    df_passifs_par_plateforme = pd.DataFrame(passifs_par_plateforme)

    fictifs_par_plateforme = list(collection.aggregate([
        #{"$match": match},
        {"$match": {"Date Extraction": date_extraction}},
        {"$match": {"Statut.libelle": "Fictif"}},
        {"$group": {"_id": "$Plateforme.id","libelle": {"$first":"$Plateforme.libelle"}, "nbre_fictifs_plateforme": {"$sum": 1}}},
        {"$sort": {"nbre_fictifs_plateforme": -1}}
    ]))
    df_fictifs_par_plateforme = pd.DataFrame(fictifs_par_plateforme)
    # ----------------------- KPI Ajouts / Suppressions / Modifs -----------------------
    res = detecter_changement_par_plateforme()
    #Le total des elements ajoutés-supprimés-modifiés dans les 4 plateformes
    TotalDetection=res["totaux"]        # {'ajouts': 12, 'suppressions': 7, 'modifications': 5}
    df_TotalDetection=pd.DataFrame([TotalDetection])
    #Le total des elements ajoutés-supprimés-modifiés dans chaque plateforme
    TotalDetectionParPlateforme=res["par_plateforme"]
    df_TotalDetectionParPlateforme=pd.DataFrame([TotalDetectionParPlateforme])
    
    # ----------------------- Écriture Excel -----------------------
    with pd.ExcelWriter("visualisation_kpi_excel.xlsx", engine='openpyxl') as writer:
        df_medecins_par_ville.to_excel(writer, sheet_name='medecins_par_ville', index=False)
        df_medecins_par_specialite.to_excel(writer, sheet_name="medecins_par_specialite", index=False)
        df_medecins_par_statut.to_excel(writer, sheet_name="mdecins_par_statut", index=False)
        df_medecins_par_plateforme.to_excel(writer, sheet_name="medecins_par_plateforme", index=False)
        df_ville_par_plateforme.to_excel(writer, sheet_name="ville_par_plateforme", index=False)
        df_specialite_par_plateforme.to_excel(writer, sheet_name="specialite_par_plateforme", index=False)
        df_actifs_par_plateforme.to_excel(writer, sheet_name="actifs_par_plateforme", index=False)
        df_passifs_par_plateforme.to_excel(writer, sheet_name="passifs_par_plateforme", index=False)
        df_fictifs_par_plateforme.to_excel(writer, sheet_name="fictifs_par_plateforme", index=False)
        df_TotalDetection.to_excel(writer,sheet_name="TotalDetection",index=False)
        df_TotalDetectionParPlateforme.to_excel(writer,sheet_name="TotalDetectionParPlateforme",index=False)
    # ----------------------- Logs console -----------------------
    print("total", total)
    print("total_spécialités", total_spécialités)
    print("total_villes", total_villes)
    print("par_specialite", medecins_par_specialite)
    print("par_ville", medecins_par_ville)
    print("par_statut", medecins_par_statut)
    print("par_plateforme", medecins_par_plateforme)
    print("ville_par_plateforme", ville_par_plateforme)
    print("specialites_par_plateforme", specialite_par_plateforme)
    print("actifs_par_plateforme", actifs_par_plateforme)
    print("fictifs_par_plateforme", fictifs_par_plateforme)
    print("TotalDetection", TotalDetection)
    print("TotalDetectionParPlateforme", TotalDetectionParPlateforme)

    # ----------------------- Payload JSON -----------------------
    result = {
        "total": total,
        "total_spécialités": total_spécialités,
        "total_villes": total_villes,
        "par_specialite": medecins_par_specialite,
        "par_ville": medecins_par_ville,
        "par_statut": medecins_par_statut,
        "par_plateforme": medecins_par_plateforme,
        "ville_par_plateforme": ville_par_plateforme,
        "specialites_par_plateforme": specialite_par_plateforme,
        "actifs_par_plateforme": actifs_par_plateforme,
        "passifs_par_plateforme": passifs_par_plateforme,
        "fictifs_par_plateforme": fictifs_par_plateforme,
        "TotalDetection": TotalDetection,
        "TotalDetectionParPlateforme": TotalDetectionParPlateforme
        
    }

    # Sauvegarde snapshot JSON à chaque appel (comme avant)
    

    return result
if __name__ == "__main__":
    data = get_kpi()  # <--- APPEL EFFECTIF
    with open("kpi_resultats.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("c'est fini!!!")

