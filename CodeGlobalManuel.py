#!/usr/bin/env python3
# job_simple.py
from run_logger import RunLogger
from datetime import datetime
import pandas as pd

# ====== imports de ton code existant ======
from DefinitionMethod_s import (
    unifier_dataframe, attribuer_ids_df_hash_simple,
    lancer_scrapping_med, lancer_scrapping_nabady, lancer_scrapping_dabadoc,
    nettoyer_dataframe_medecins, detecter_changement_par_plateforme
)
from insertion import inserer_dataframe
from ConnexionMongo_DB import get_db
from CalculerKPi import get_kpi
# ==========================================

def main():
    # IMPORTANT: même job_name que l’automatique pour unifier le suivi
    rl = RunLogger("scrape_all_minimal")
    try:
        # Tag "manual" pour distinguer l’origine dans runs.tags
        rl.start(tags=["manual"])
        rl.event("Début du job manuel", step="start")

        date_extraction = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 1) Scraping — décommente ce que tu veux activer
        rl.event("Scrape med", step="scrape_med")
        df1 = lancer_scrapping_med(); df1["Date Extraction"] = date_extraction
        rl.add_rows("med.ma", len(df1))

        rl.event("Scrape nabady", step="scrape_nabady")
        df2 = lancer_scrapping_nabady(); df2["Date Extraction"] = date_extraction
        rl.add_rows("nabady", len(df2))

        # rl.event("Scrape dabadoc", step="scrape_dabadoc")
        # df3 = lancer_scrapping_dabadoc(); df3["Date Extraction"] = date_extraction
        # rl.add_rows("dabadoc", len(df3))

        # 2) Docdialy depuis Excel (si besoin)
        # rl.event("Lire Docdialy Excel", step="docdialy_read")
        # df4 = pd.read_excel("/home/salma/monitor_app/data/Fichier_DocDialy_ma.xlsx")
        # df4 = nettoyer_dataframe_medecins(df4, "Docdialy")
        # df4["Date Extraction"] = date_extraction
        # rl.add_rows("docdialy", len(df4))

        # 3) Unifier schéma
        rl.event("Unifier schéma", step="unify")
        # if 'df1' in locals(): df1 = unifier_dataframe(df1, "Med")
        df2 = unifier_dataframe(df2, "Nabady")
        # if 'df3' in locals(): df3 = unifier_dataframe(df3, "Dabadoc")
        # if 'df4' in locals(): df4 = unifier_dataframe(df4, "Docdialy")

        # 4) Concaténer (si plusieurs DF activés)
        rl.event("Concat", step="concat")
        # frames = [x for x in [locals().get('df1'), df2, locals().get('df3'), locals().get('df4')] if x is not None]
        df_final = pd.concat([df1,df2], ignore_index=True) 
        

        # 5) IDs stables + insertion Mongo (même place que l’automatique)
        rl.event("Attribuer IDs", step="ids")
        db = get_db()
        df_final, id_maps = attribuer_ids_df_hash_simple(df_final, db, collection_name="collection_globale")

        rl.event("Insérer dans Mongo (collection_globale)", step="insert")
        inserer_dataframe(df_final, db, "collection_globale")

        # 6) (Optionnel) Détection & KPI — on journalise mais on ne bloque pas le run s’il n’y a pas besoin
        try:
            rl.event("Calcul KPI+ Dashboard", step="kpi")
            _ = create_app()  # si ta fonction renvoie un dict/df, pas besoin de l’utiliser ici
        except Exception as _kpi_err:
            rl.event(f"KPI non calculés: {_kpi_err}", level="WARNING", step="kpi")

        rl.event("Terminé", step="done")
        rl.finish_success()

    except Exception as e:
        rl.event(f"Erreur globale: {e}", level="ERROR", step="global")
        rl.finish_error(e)
        raise

if __name__ == "__main__":
    main()
