import requests,time,json, pandas as pd,re
from datetime import datetime,timedelta,timezone
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
import os
import hashlib, unicodedata, numpy as np
################---------------------Fichier Med---------------###############################
def get_medecinsMed():
    session = requests.Session()
    # Faire une première requête GET pour récupérer le cookie PHPSESSID
    url_initial = "https://www.med.ma/medecin/recherche/"
    response1 = session.get(url_initial)
    # 3. Récupérer le PHPSESSID
    phpsessid = response1.cookies.get("PHPSESSID")
    cookie=response1.cookies.get("cc_cookie")
    print("PHPSESSID récupéré automatiquement:", phpsessid)
    url = "https://www.med.ma/pagesmd_load.php"
    ids_uniques=set()
    headers = {
        "Accept": "*/*",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.med.ma",
        "Referer": "https://www.med.ma/medecin/recherche/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "X-Requested-With": "XMLHttpRequest"
    }
    cookies = {
        "PHPSESSID":phpsessid,
        "cc_cookie":cookie  
    }
    villes_maroc = [
        "Casablanca", "Rabat", "Fès", "Marrakech", "Tanger", "Agadir",
        "Meknès", "Oujda", "Tétouan", "El Jadida", "Nador", "Khouribga",
        "Kénitra", "Témara", "Salé", "Mohammedia", "Béni Mellal", 
        "Laâyoune","Safi","Sefrou","Benslimane","Ouarzazate","Berrechid","Settat",
        "Ait Ouabelli Tata","Benslimane","Tiznit","Al kelaa","Taroudannt","Guelmim"
    ]
    #  Fonction pour détecter la ville dans une adresse
    def detecter_ville(adresse, liste_villes):
        adresse = str(adresse).lower()
        for ville in liste_villes:
            if ville.lower() in adresse:
                return ville
        return ""

    start = 0
    step = 30
    all_data = []
    
   

    while True:
        print(f"\n Récupération de {step} médecins à partir de start={start}...")

        payload = {
            "start": start,
            "spe": "",
            "gov": "",
            "del": "",
            "nearest": "0",
            "speciality": "",
            "city_country": "ma",
            "geo_country": "",
            "proximity": "0",
            "sponsor": "",
            "act_id": "",
            "nbMainList": "704"
        }

        response = requests.post(url, headers=headers, cookies=cookies, data=payload)
        soup = BeautifulSoup(response.text, "html.parser")

        blocs = soup.find_all("div", class_="card-doctor-block")
        print(f" Médecins trouvés sur cette page : {len(blocs)}")

        if not blocs:
            print(" Fin de l'extraction : plus de médecins.")
            break

        for bloc in blocs:
            nom_tag = bloc.select_one(".list__label--name")
            nom = nom_tag.text.strip() if nom_tag else ""
            specialite = bloc.select_one(".list__label--spee")
            adresse_tag = bloc.select_one(".list__label--adr")
            adresse = adresse_tag.text.strip() if adresse_tag else ""
            ville = detecter_ville(adresse, villes_maroc)
            a_tag = bloc.find('a')  # ou soup.find_all('a') si plusieurs
            if a_tag and a_tag.has_attr('href'):
                                    lien = a_tag['href']
                                    element= lien.rstrip('/').split('/')[-1]
                                    id_profil=element.rstrip('-').split('-')[-1]
            #presentation = bloc.select_one(".list__bio")
            actes = [a.text.strip() for a in bloc.select("div.list__acts a.tagcloud")]

            fiche = bloc.find("a", class_="button__rdv")
            texte_fiche = fiche.text.strip() if fiche else ""
            statut = "Fictif" if "voir fiche" in texte_fiche.lower() else "Actif"

            profil_tag = bloc.find("a", href=True)
            profil_url = profil_tag["href"] if profil_tag else ""
            telephone = ""
            whatsapp = ""

            if profil_url:
                try:
                    res_profil = requests.get(profil_url, headers=headers, timeout=10)
                    soup_profil = BeautifulSoup(res_profil.text, "html.parser")

                    # Téléphones
                    tel_tags = soup_profil.select("a.calltel span")
                    telephones = [tel.get_text(strip=True) for tel in tel_tags]
                    telephone = " / ".join(telephones)

                    # WhatsApp
                    wa_tags = soup_profil.select("a.callwhatsapp span")
                    whatsapps = [wa.get_text(strip=True) for wa in wa_tags]
                    whatsapp = " / ".join(whatsapps)
                    
                    #Qualification Professioneelles
                    qualification = ""
                    qualif_h3 = soup_profil.find("h3", string="Qualification professionnelle")
                    if qualif_h3:
                        p_tagQ = qualif_h3.find_next_sibling("p")
                    if p_tagQ:
                        qualification = p_tagQ.get_text(separator=' ', strip=True)
                        
                    #  Langues parlées
                    langues_=""
                    langues_h3 = soup_profil.find("h3", string=lambda t: t and "Langues parlées" in t)
                    if langues_h3:
                        div_langues= langues_h3.find_next_sibling("div")
                        if div_langues:
                            langues_ = div_langues.get_text(separator=", ", strip=True)
                except Exception as e:
                    print(f" Erreur accès profil {profil_url} : {e}")
        
            
           
              
            all_data.append({
                "ID Medecin":id_profil,
                "Nom Complet": nom,
                "Spécialité": specialite.text.strip() if specialite else "",
                "Adresse": adresse,
                "Ville": ville,
                "Soins et Actes": ", ".join(actes),
                "Téléphone": telephone,
                "WhatsApp": whatsapp,
                "Statut": statut,
                "Lien Profil":profil_url,
                "Langues Parlées":langues_,
                "Diplomes et Formations":qualification
                
            })
            print(profil_url,langues_,qualification)
        if len(blocs) < step:
            print(" Dernière page atteinte. Fin de l'extraction.")
            break

        start += step
        time.sleep(1)
    dfMed = pd.DataFrame(all_data)
    dfMed.to_excel(r"C:\Users\HP\Desktop\Data_Folder\Fichier_Med_ma_Avant_Nettoyage.xlsx",index=False)
    print(f"\n Extraction terminée. Nombre total de médecins : {len(dfMed)}")
    return dfMed
def get_medecinsMed_actifs_passifs():
    session = requests.Session()
    # Faire une première requête GET pour récupérer le cookie PHPSESSID
    url_initial = "https://www.med.ma/medecin/recherche/"
    response1 = session.get(url_initial)
    # 3. Récupérer le PHPSESSID
    phpsessid = response1.cookies.get("PHPSESSID")
    cookie=response1.cookies.get("cc_cookie")
    print("PHPSESSID récupéré automatiquement:", phpsessid)
    url = "https://www.med.ma/pagesmd_load.php"
    ids_uniques=set()
    headers = {
        "Accept": "*/*",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.med.ma",
        "Referer": "https://www.med.ma/medecin/recherche/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "X-Requested-With": "XMLHttpRequest"
    }
    cookies = {
        "PHPSESSID":phpsessid,
        "cc_cookie":cookie  
    }
    villes_maroc = [
        "Casablanca", "Rabat", "Fès", "Marrakech", "Tanger", "Agadir",
        "Meknès", "Oujda", "Tétouan", "El Jadida", "Nador", "Khouribga",
        "Kénitra", "Témara", "Salé", "Mohammedia", "Béni Mellal", 
        "Laâyoune","Safi","Sefrou","Benslimane","Ouarzazate","Berrechid","Settat",
        "Ait Ouabelli Tata","Benslimane","Tiznit","Al kelaa","Taroudannt","Guelmim"
    ]
    #  Fonction pour détecter la ville dans une adresse
    def detecter_ville(adresse, liste_villes):
        adresse = str(adresse).lower()
        for ville in liste_villes:
            if ville.lower() in adresse:
                return ville
        return ""

    start = 0
    step = 30
    all_data = []
    
   

    while True:
        print(f"\n Récupération de {step} médecins à partir de start={start}...")

        payload = {
            "start": start,
            "spe": "",
            "gov": "",
            "del": "",
            "nearest": "0",
            "speciality": "",
            "city_country": "ma",
            "geo_country": "",
            "proximity": "0",
            "sponsor": "",
            "act_id": "",
            "nbMainList": "704"
        }

        response = requests.post(url, headers=headers, cookies=cookies, data=payload)
        soup = BeautifulSoup(response.text, "html.parser")

        blocs = soup.find_all("div", class_="card-doctor-block")
        print(f" Médecins trouvés sur cette page : {len(blocs)}")

        if not blocs:
            print(" Fin de l'extraction : plus de médecins.")
            break

        for bloc in blocs:
            nom_tag = bloc.select_one(".list__label--name")
            nom = nom_tag.text.strip() if nom_tag else ""
            specialite = bloc.select_one(".list__label--spee")
            adresse_tag = bloc.select_one(".list__label--adr")
            adresse = adresse_tag.text.strip() if adresse_tag else ""
            ville = detecter_ville(adresse, villes_maroc)
            a_tag = bloc.find('a')  # ou soup.find_all('a') si plusieurs
            if a_tag and a_tag.has_attr('href'):
                                    lien = a_tag['href']
                                    element= lien.rstrip('/').split('/')[-1]
                                    id_profil=element.rstrip('-').split('-')[-1]
            #presentation = bloc.select_one(".list__bio")
            actes = [a.text.strip() for a in bloc.select("div.list__acts a.tagcloud")]

            fiche = bloc.find("a", class_="button__rdv")
            texte_fiche = fiche.text.strip() if fiche else ""
            statut = "Fictif" if "voir fiche" in texte_fiche.lower() else "Actif"

            profil_tag = bloc.find("a", href=True)
            profil_url = profil_tag["href"] if profil_tag else ""
            telephone = ""
            whatsapp = ""

            if profil_url:
                try:
                    res_profil = requests.get(profil_url, headers=headers, timeout=10)
                    soup_profil = BeautifulSoup(res_profil.text, "html.parser")

                    # Téléphones
                    tel_tags = soup_profil.select("a.calltel span")
                    telephones = [tel.get_text(strip=True) for tel in tel_tags]
                    telephone = " / ".join(telephones)

                    # WhatsApp
                    wa_tags = soup_profil.select("a.callwhatsapp span")
                    whatsapps = [wa.get_text(strip=True) for wa in wa_tags]
                    whatsapp = " / ".join(whatsapps)
                    
                    #Qualification Professioneelles
                    qualification = ""
                    qualif_h3 = soup_profil.find("h3", string="Qualification professionnelle")
                    if qualif_h3:
                        p_tagQ = qualif_h3.find_next_sibling("p")
                    if p_tagQ:
                        qualification = p_tagQ.get_text(separator=' ', strip=True)
                        
                    #  Langues parlées
                    langues_=""
                    langues_h3 = soup_profil.find("h3", string=lambda t: t and "Langues parlées" in t)
                    if langues_h3:
                        div_langues= langues_h3.find_next_sibling("div")
                        if div_langues:
                            langues_ = div_langues.get_text(separator=", ", strip=True)
                except Exception as e:
                    print(f" Erreur accès profil {profil_url} : {e}")
            if statut in ["Actif","Passif"]:
                all_data.append({
                    "ID Medecin":id_profil,
                    "Nom Complet": nom,
                    "Spécialité": specialite.text.strip() if specialite else "",
                    "Adresse": adresse,
                    "Ville": ville,
                    "Soins et Actes": ", ".join(actes),
                    "Téléphone": telephone,
                    "WhatsApp": whatsapp,
                    "Statut": statut,
                    "Lien Profil":profil_url,
                    "Langues Parlées":langues_,
                    "Diplomes et Formations":qualification
                    
            })
                print(profil_url,langues_,qualification)
        if len(blocs) < step:
            print(" Dernière page atteinte. Fin de l'extraction.")
            break

        start += step
        time.sleep(1)
    dfMed = pd.DataFrame(all_data)
    dfMed.to_excel(r"C:\Users\HP\Desktop\Data_Folder\Fichier_Med_ma_Avant_Nettoyage.xlsx",index=False)
    print(f"\n Extraction terminée. Nombre total de médecins : {len(dfMed)}")
    return dfMed
#################---------------------Fichier Nabady------------###########################
def get_medecinsNabady():
    url = "https://api.nabady.ma/api/praticien_centre_soins/medecin/search"
    headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "cache-control": "no-cache",
    "content-type": "application/json",
    "origin": "https://nabady.ma",
    "pragma": "no-cache",
    "referer": "https://nabady.ma/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
}
    page=1
    total=0
    limite=datetime.now(timezone.utc)-timedelta(days=45)
    limite_=limite.replace(tzinfo=None)
    ids_uniques=set()
    all_Medecins=[]
    Duree=45
    payload={
    "consultation":"15",
    "page":page,
    "result":200,
    "isIframe":False,
    "referrer":"",
    "domain":"ma"
}
    while True:
        print(f"Traitement de la page : {page}")
        payload['page']=page  
        response=requests.post(url,headers=headers,json=payload,timeout=10)
        if response.status_code!=200:
            print("Erreur HTTP",response.status_code)
            break
        data_response=response.json().get("data",[])
        if not data_response:
            print("Fin des resultats")   
            break 
        for element in data_response:
            bloc=element.get("0",{})
            praticien=bloc.get("praticien",{})    
            if not praticien:
                continue
            Statut=""
            try:
                raw_connexion=praticien.get("lastcnx","")
                if raw_connexion is not None:
                    time_connexion=datetime.fromisoformat(raw_connexion)
                    if time_connexion.tzinfo is not None:
                        time_connexion=time_connexion.replace(tzinfo=None)
                        if time_connexion>=limite_:
                            Statut="Actif"
                        else:
                            Statut="Passif"  
                else:
                    Statut="Fictif"
                    time_connexion=datetime(1900,1,1)
            except Exception as e:
                Statut=""
            id_medecin=praticien.get("id","")
            civilite=praticien.get("civilite","")or ""
            prenom=praticien.get("firstname","")or ""
            nom=praticien.get("lastname","")or""
            email=praticien.get("email","")or ""
            if email:
                email=email.strip()
            sexe=praticien.get("sexe","")
            tel=praticien.get("tel","")
            Fix=praticien.get("fix","")
            adresse=praticien.get("adresse","")
            ville=praticien.get("ville",{})or{}
            ville_name=ville.get("name","")
            ville_id=ville.get("id","")
            try:
                date_naissance=praticien.get("dateNaissance","") 
                if date_naissance is not None:
                    time_date_naissance = datetime.fromisoformat(date_naissance)
                    if time_date_naissance.tzinfo is not None:
                        time_date_naissance = time_date_naissance.replace(tzinfo=None)
                else:
                    time_date_naissance=""
            except Exception as e:
                time_date_naissance=""
            raw_presentation=praticien.get("presentation","")
            presentation=raw_presentation.replace("\n"," ").strip() if isinstance(raw_presentation,str)else " "
            photo=praticien.get("img","")
            acces_specialite=praticien.get("centreSoinSpecialiteUsers",[])
            for item in acces_specialite:
                specialite=item.get("specialite",{})
                specialite_name=specialite.get("name","")
                specialite_id=specialite.get("id","")
            acces_langues=praticien.get("langue",[])
            langues=[c.get("langue")for c in acces_langues if c.get("langue")]
        
      
            if id_medecin not in ids_uniques:
                ids_uniques.add(id_medecin)
                all_Medecins.append({
                "ID Medecin":id_medecin,
                "Statut":Statut,
                "Civilite":civilite,
                "Nom":nom,
                "Prénom":prenom,
                "Sexe":sexe,
                "Date de Naissance":time_date_naissance,
                "Email":email,
                "Téléphone":tel,
                "Fix":Fix,
                "ID Spécialité":specialite_id,
                "Spécialité":specialite_name,
                "ID Ville":ville_id,
                "Ville":ville_name,
                "Photo URL":photo,
                "Adresse":adresse,
                "Présentation":presentation,
                "Langues Parlées":langues,
                "Date de Connexion":time_connexion,
                "Duree":Duree
              
            })
                total+=1
                print( "1","|Prénom-",Statut,"|",Fix,"|" ,"|",limite_,time_connexion)
        print(f"Total dans la page{total}")
        time.sleep(2)
        page+=1  
    dfNabady=pd.DataFrame(all_Medecins)
    
    dfNabady.to_excel(r"C:\Users\HP\Desktop\Data_Folder\Fichier_Nabady_ma_Avant_Nettoyage.xlsx",index=False)
    return dfNabady
def get_medecinsNabady_actifs_passifs():
    url = "https://api.nabady.ma/api/praticien_centre_soins/medecin/search"
    headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "cache-control": "no-cache",
    "content-type": "application/json",
    "origin": "https://nabady.ma",
    "pragma": "no-cache",
    "referer": "https://nabady.ma/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
}
    page=1
    total=0
    limite=datetime.now(timezone.utc)-timedelta(days=45)
    limite_=limite.replace(tzinfo=None)
    ids_uniques=set()
    all_Medecins=[]
    Duree=45
    payload={
    "consultation":"15",
    "page":page,
    "result":200,
    "isIframe":False,
    "referrer":"",
    "domain":"ma"
}
    while True:
        print(f"Traitement de la page : {page}")
        payload['page']=page  
        response=requests.post(url,headers=headers,json=payload,timeout=10)
        if response.status_code!=200:
            print("Erreur HTTP",response.status_code)
            break
        data_response=response.json().get("data",[])
        if not data_response:
            print("Fin des resultats")   
            break 
        for element in data_response:
            bloc=element.get("0",{})
            praticien=bloc.get("praticien",{})    
            if not praticien:
                continue
            Statut=""
            try:
                raw_connexion=praticien.get("lastcnx","")
                if raw_connexion is not None:
                    time_connexion=datetime.fromisoformat(raw_connexion)
                    if time_connexion.tzinfo is not None:
                        time_connexion=time_connexion.replace(tzinfo=None)
                        if time_connexion>=limite_:
                            Statut="Actif"
                        else:
                            Statut="Passif"  
                else:
                    Statut="Fictif"
                    time_connexion=datetime(1900,1,1)
            except Exception as e:
                Statut=""
            id_medecin=praticien.get("id","")
            civilite=praticien.get("civilite","")or ""
            prenom=praticien.get("firstname","")or ""
            nom=praticien.get("lastname","")or""
            email=praticien.get("email","")or ""
            if email:
                email=email.strip()
            sexe=praticien.get("sexe","")
            tel=praticien.get("tel","")
            Fix=praticien.get("fix","")
            adresse=praticien.get("adresse","")
            ville=praticien.get("ville",{})or{}
            ville_name=ville.get("name","")
            ville_id=ville.get("id","")
            try:
                date_naissance=praticien.get("dateNaissance","") 
                if date_naissance is not None:
                    time_date_naissance = datetime.fromisoformat(date_naissance)
                    if time_date_naissance.tzinfo is not None:
                        time_date_naissance = time_date_naissance.replace(tzinfo=None)
                else:
                    time_date_naissance=""
            except Exception as e:
                time_date_naissance=""
            raw_presentation=praticien.get("presentation","")
            presentation=raw_presentation.replace("\n"," ").strip() if isinstance(raw_presentation,str)else " "
            photo=praticien.get("img","")
            acces_specialite=praticien.get("centreSoinSpecialiteUsers",[])
            for item in acces_specialite:
                specialite=item.get("specialite",{})
                specialite_name=specialite.get("name","")
                specialite_id=specialite.get("id","")
            acces_langues=praticien.get("langue",[])
            langues=[c.get("langue")for c in acces_langues if c.get("langue")]
        
            if Statut in ["Actif","Passif"]:
                if id_medecin not in ids_uniques:
                    ids_uniques.add(id_medecin)
                    all_Medecins.append({
                    "ID Medecin":id_medecin,
                    "Statut":Statut,
                    "Civilite":civilite,
                    "Nom":nom,
                    "Prénom":prenom,
                    "Sexe":sexe,
                    "Date de Naissance":time_date_naissance,
                    "Email":email,
                    "Téléphone":tel,
                    "Fix":Fix,
                    "ID Spécialité":specialite_id,
                    "Spécialité":specialite_name,
                    "ID Ville":ville_id,
                    "Ville":ville_name,
                    "Photo URL":photo,
                    "Adresse":adresse,
                    "Présentation":presentation,
                    "Langues Parlées":langues,
                    "Date de Connexion":time_connexion,
                    "Duree":Duree
                
                })
                total+=1
                print( "1","|Prénom-",Statut,"|",Fix,"|" ,"|",limite_,time_connexion)
        print(f"Total dans la page{total}")
        time.sleep(2)
        page+=1  
    dfNabady=pd.DataFrame(all_Medecins)
    
    dfNabady.to_excel(r"C:\Users\HP\Desktop\Data_Folder\Fichier_Nabady_ma_Avant_Nettoyage.xlsx",index=False)
    return dfNabady
##################------------------Fichier Dabadoc------------############################
def get_medecinsDabadoc():
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    }
    # 🔧 Fonction pour extraire nom, spécialité et ville
    def extract_nom_specialite_ville(alt_text):
        alt_text = alt_text.strip()
        parts = alt_text.split(',')
        if len(parts) < 3:
            return "", "", ""
        nom = parts[0].strip()
        ville = parts[-1].strip()
        specialite = ','.join(parts[1:-1]).strip()
        return nom, specialite, ville
    # 
    donnees_medecins = []
    

    #  Boucle sur les pages
    for page in range(1, 1286):
        url = f"https://www.dabadoc.com/recherche/page/{page}?button=&country=MA" if page > 1 else "https://www.dabadoc.com/recherche?button=&country=MA"
        print(f"\n🔎 Page {page} → {url}")
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Erreur de chargement : {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        blocs = soup.find_all("div", class_="search_doc_row")
        print(f" Médecins trouvés : {len(blocs)}")

        for bloc in blocs:
            try:
                #Accéder au profil
                a_tag = bloc.find("a", href=True)
                profil_link = urljoin("https://www.dabadoc.com", a_tag['href']) if a_tag else ""
                
                result_box = bloc.find("div", class_="result-box rounded")
                identifiant = result_box.get("id") if result_box else ""
                img_tag = bloc.find("img", alt=True, attrs={"data-src": True})
                alt_text = img_tag['alt'] if img_tag else "Non trouvé"
                photo_url = img_tag['data-src'] if img_tag else "Aucune"
                nom, specialite, ville = extract_nom_specialite_ville(alt_text)

                statut = "Fictive"
            

                horaire, telephone, adresse = "", "", ""

                if bloc.find("div", class_="availabilities-box"):
                        statut = "Actif"
                elif "Prendre RDV en Video" in bloc.text or bloc.find("a", class_="btn btn-success mt-1 text-white"):
                        statut = "Passif"
                elif bloc.find("div",class_="no-appointments-msg"):
                        statut = "Fictif"
                    

                #  Scraping du profil pour plus de détails
                
                print(f" Profil : {profil_link}")
                try:
                    profil_res = requests.get(profil_link, headers=headers)
                    if profil_res.status_code == 200:
                        profil_soup = BeautifulSoup(profil_res.text, "html.parser")
                        adresse_div = profil_soup.find("div", class_="card-text")
                    if adresse_div:
                            adresse = adresse_div.get_text(strip=True, separator=' ')
                    #Extraire les actes     
                    SoinsActes = []
                    soins_badges = profil_soup.find_all("a", class_="badge badge-secondary p-2 mb-1")
                    for soin in soins_badges:
                        texte = soin.get_text(strip=True)
                        if texte and isinstance(texte, str):
                            SoinsActes.append(texte)
                    #Extraire les diplomes,certificats et formations à la fois
                    diplomes = []
                    for balise in profil_soup.find_all("i", class_="fa fa-circle"):
                        texte = balise.next_sibling
                        if texte and isinstance(texte, str):
                            diplomes.append(texte.strip()) 
                    #Extraire le numero de téléphone
                    phone_btn = profil_soup.find("a", id="phone-number-btn")
                    if phone_btn and "href" in phone_btn.attrs:
                            tel_href = phone_btn["href"]
                            if tel_href.startswith("tel:"):
                                    telephone = tel_href.replace("tel:", "").strip()
                    #Extraire les langues
                    Langues_Parlees = ""
                    icone_langues = profil_soup.find("i", class_="fa fa-language")
                    if icone_langues:
                        div_card_text = icone_langues.find_next("div", class_="card-text")
                        if div_card_text:
                            Langues_Parlees = div_card_text.get_text(strip=True)
                
                except Exception as e:
                        print(" Erreur profil :", e)
                
                # Ajout dans la liste
                donnees_medecins.append({
                    "ID Medecin": identifiant,
                    "Nom Complet": nom,
                    "Spécialité": specialite,
                    "Ville": ville,
                    "Lien Profil": profil_link,
                    "Photo URL": photo_url,
                    "Statut": statut,
                    "Téléphone": telephone,
                    "Adresse": adresse,
                    "Diplomes et Formations":diplomes,
                    "Soins et Actes":SoinsActes,
                    "Langues Parlées":Langues_Parlees
                })

                print( identifiant, "|", specialite, "|", ville, "|", statut, "|", adresse,"|",datetime.now(),"|",diplomes,"|",SoinsActes,"|",Langues_Parlees)
            except Exception as err:
                print(" Erreur bloc médecin :", err)

        time.sleep(1.5)  # Pause pour éviter le blocage IP

    # Export vers Excel 
    dfDabadoc = pd.DataFrame(donnees_medecins)
    dfDabadoc .to_excel(r"C:\Users\HP\Desktop\Data_Folder\Fichier_Dabadoc_ma_Avant_Nettoyage.xlsx", index=False)
    print("\n Export terminé ! Nombre total de médecins :", len(dfDabadoc ))
    return dfDabadoc 
def get_medecinsDabadoc_actifs_passifs():
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    }
    # 🔧 Fonction pour extraire nom, spécialité et ville
    def extract_nom_specialite_ville(alt_text):
        alt_text = alt_text.strip()
        parts = alt_text.split(',')
        if len(parts) < 3:
            return "", "", ""
        nom = parts[0].strip()
        ville = parts[-1].strip()
        specialite = ','.join(parts[1:-1]).strip()
        return nom, specialite, ville
    # 
    donnees_medecins = []
    

    #  Boucle sur les pages
    for page in range(1, 1286):
        url = f"https://www.dabadoc.com/recherche/page/{page}?button=&country=MA" if page > 1 else "https://www.dabadoc.com/recherche?button=&country=MA"
        print(f"\n🔎 Page {page} → {url}")
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Erreur de chargement : {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        blocs = soup.find_all("div", class_="search_doc_row")
        print(f" Médecins trouvés : {len(blocs)}")

        for bloc in blocs:
            try:
                #Accéder au profil
                a_tag = bloc.find("a", href=True)
                profil_link = urljoin("https://www.dabadoc.com", a_tag['href']) if a_tag else ""
                
                result_box = bloc.find("div", class_="result-box rounded")
                identifiant = result_box.get("id") if result_box else ""
                img_tag = bloc.find("img", alt=True, attrs={"data-src": True})
                alt_text = img_tag['alt'] if img_tag else "Non trouvé"
                photo_url = img_tag['data-src'] if img_tag else "Aucune"
                nom, specialite, ville = extract_nom_specialite_ville(alt_text)

                statut = "Fictive"
            

                horaire, telephone, adresse = "", "", ""

                if bloc.find("div", class_="availabilities-box"):
                        statut = "Actif"
                elif "Prendre RDV en Video" in bloc.text or bloc.find("a", class_="btn btn-success mt-1 text-white"):
                        statut = "Passif"
                elif bloc.find("div",class_="no-appointments-msg"):
                        statut = "Fictif"
                    
                if statut in ["Actif","Passif"]:
                #  Scraping du profil pour plus de détails
                
                    print(f" Profil : {profil_link}")
                    try:
                        profil_res = requests.get(profil_link, headers=headers)
                        if profil_res.status_code == 200:
                            profil_soup = BeautifulSoup(profil_res.text, "html.parser")
                            adresse_div = profil_soup.find("div", class_="card-text")
                        if adresse_div:
                                adresse = adresse_div.get_text(strip=True, separator=' ')
                        #Extraire les actes     
                        SoinsActes = []
                        soins_badges = profil_soup.find_all("a", class_="badge badge-secondary p-2 mb-1")
                        for soin in soins_badges:
                            texte = soin.get_text(strip=True)
                            if texte and isinstance(texte, str):
                                SoinsActes.append(texte)
                        #Extraire les diplomes,certificats et formations à la fois
                        diplomes = []
                        for balise in profil_soup.find_all("i", class_="fa fa-circle"):
                            texte = balise.next_sibling
                            if texte and isinstance(texte, str):
                                diplomes.append(texte.strip()) 
                        #Extraire le numero de téléphone
                        phone_btn = profil_soup.find("a", id="phone-number-btn")
                        if phone_btn and "href" in phone_btn.attrs:
                                tel_href = phone_btn["href"]
                                if tel_href.startswith("tel:"):
                                        telephone = tel_href.replace("tel:", "").strip()
                        #Extraire les langues
                        Langues_Parlees = ""
                        icone_langues = profil_soup.find("i", class_="fa fa-language")
                        if icone_langues:
                            div_card_text = icone_langues.find_next("div", class_="card-text")
                            if div_card_text:
                                Langues_Parlees = div_card_text.get_text(strip=True)
                    
                    except Exception as e:
                            print(" Erreur profil :", e)
                    
                    # Ajout dans la liste
                    donnees_medecins.append({
                        "ID Medecin": identifiant,
                        "Nom Complet": nom,
                        "Spécialité": specialite,
                        "Ville": ville,
                        "Lien Profil": profil_link,
                        "Photo URL": photo_url,
                        "Statut": statut,
                        "Téléphone": telephone,
                        "Adresse": adresse,
                        "Diplomes et Formations":diplomes,
                        "Soins et Actes":SoinsActes,
                        "Langues Parlées":Langues_Parlees
                    })

                    print( identifiant, "|", specialite, "|", ville, "|", statut, "|", adresse,"|",datetime.now(),"|",diplomes,"|",SoinsActes,"|",Langues_Parlees)
            except Exception as err:
                print(" Erreur bloc médecin :", err)

        time.sleep(1.5)  # Pause pour éviter le blocage IP

    # Export vers Excel 
    dfDabadoc = pd.DataFrame(donnees_medecins)
    dfDabadoc .to_excel(r"C:\Users\HP\Desktop\Data_Folder\Fichier_Dabadoc_ma_Avant_Nettoyage.xlsx", index=False)
    print("\n Export terminé ! Nombre total de médecins :", len(dfDabadoc ))
    return dfDabadoc 
#############----------------------Fichier Docdialy----------#####################
def get_medecinsDocdialy():

    url ="https://api.docdialy.com/api/website/doctors?doctor=&speciality=&city=&country=1&type=is_office&page=1&offset=12&order=asc&order_by=name"

    headers = {
        "accept": "*/*",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "authorization": "Bearer null",
        "cache-control": "no-cache",
        "lang": "fr",
        "origin": "https://www.docdialy.com",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://www.docdialy.com/",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    }

    payload = {
        "doctor": "",
        "speciality": "",
        "city": "",
        "country": "ma",
        "type":"is_office",
        "page": 1  ,
        "offset": 12,
        "order": "asc",
        "order_by": "name"
    }
    all_medecins=[]
    while True:
        print(f"Récupération de la page : {payload['page']}")
        response=requests.get(url,params=payload,headers=headers)
        
        if response.status_code!=200:
            print("Erreur HTTP:",response.status_code)
            print(response.text)
            break
        data=response.json().get("data",{})
        medecins=data.get("data",[])
        if not medecins:
            print("Fin de la pagination")
            break
        for med in medecins:
            identifiant=med.get("id")
            first=med.get("first_name","")or""
            last=med.get("last_name","")or""
            Nom_Complet=med.get("name","").strip()
            Statut=""
            try:
                DocteurdeDocDialy=med.get("is_docdialy_doctor")
                if DocteurdeDocDialy==True:
                    Statut="Actif"
                else:
                    Statut="Fictif"
            except Exception as e:
                Statut="Fictif"
            
            specialites=[s["id"]+","+s["name"] for s in med.get("specialities",[])]
            lien=med.get("slug","")
            adresse=med.get("address","")
            email=med.get("email","")or""
            if email:
                email=email.strip()
            urls={
                "certificats":f"https://api.docdialy.com/api/website/doctors/{identifiant}/certifications",
                "languages":f"https://api.docdialy.com/api/website/doctors/{identifiant}/languages",
                "care_acts":f"https://api.docdialy.com/api/website/doctors/{identifiant}/care-acts",
                "profile":f"https://api.docdialy.com/api/website/doctors/{identifiant}/profile",
                "presentation":f"https://api.docdialy.com/api/website/doctors/{identifiant}/schema-doctor"
                }
            def AccederDetails(name_url):
                name,url =name_url
                try:
                    response=requests.get(url,headers=headers,timeout=10)
                    if response.status_code==200:
                        return name,response.json()
                    else:
                        return name,{}
                except Exception as e:
                    return name,{}
                    #ICI J'ai utilisé la methode svt pour gangner du temps et accéeder 
                    #Aux différentes URL en parallèle
            with ThreadPoolExecutor() as executor:
                results=dict(executor.map(AccederDetails,urls.items()))
            #Certificats et diplomes:
            data_certifs=results.get("certificats",{}).get("data",{}).get("certifications",[])
            label_certificats=[c.get("label")for c in data_certifs if c.get("label")]
            #Langues
            data_langues=results.get("languages",{}).get("data",{}).get("languages",[])
            label_langues=[l.get("label")for l in data_langues if l.get("label")]
            #Actes
            data_actes=results.get("care_acts",{}).get("data",{}).get("care_acts",[])
            label_actes=[a.get("name")for a in data_actes if a.get("name")]
            #Presentation
            data_presentation=results.get("presentation",{}).get("data",{}).get("presentation","")
            Sexe=results.get("profile",{}).get("data",{}).get("gender","")or ""
            if Sexe.lower()=="male":
                Sexe="Homme"
            elif Sexe.lower()=="female":
                Sexe="Femme"
            else:
                Sexe=""
            all_medecins.append({
                    "ID Medecin":identifiant,
                    "Civilité":med.get("professional_status").capitalize(),
                    "Statut":Statut,
                    "Prénom":first,
                    "Nom":last,
                    "Nom Complet":Nom_Complet,
                    "Sexe":Sexe,
                    "Télephone":med.get("phone"),
                    "Email":email,
                    "Adresse":adresse,
                    "ID Ville":med.get("city_id",""),
                    "Ville":med.get("city",""),
                    "Spécialités":specialites,
                    "Lien Profil":lien,
                    "Soins et Actes":label_actes,
                    "Langues Parlées":label_langues,
                    "Diplomes et Formations":label_certificats,
                    "Presentation":data_presentation
                    
                })
                
            print("A","|",adresse,"|",specialites,"|",label_actes,"|",label_certificats,"|",label_langues,"|",data_presentation)
                
        dfDocdialy=pd.DataFrame(all_medecins)
        dfDocdialy.to_excel(r"C:\Users\HP\Desktop\Data_Folder\Fichier_DocDialy_ma_Avant_Nettoyage.xlsx",index=False)        
                
        payload['page']+=1
        time.sleep(1)       
    return dfDocdialy
def get_medecinsDocdialy_actifs_passifs():
    url ="https://api.docdialy.com/api/website/doctors?doctor=&speciality=&city=&country=1&type=is_office&page=1&offset=12&order=asc&order_by=name"

    headers = {
        "accept": "*/*",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "authorization": "Bearer null",
        "cache-control": "no-cache",
        "lang": "fr",
        "origin": "https://www.docdialy.com",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://www.docdialy.com/",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    }

    payload = {
        "doctor": "",
        "speciality": "",
        "city": "",
        "country": "ma",
        "type":"is_office",
        "page": 1  ,
        "offset": 12,
        "order": "asc",
        "order_by": "name"
    }
    all_medecins=[]
    while True:
        print(f"Récupération de la page : {payload['page']}")
        response=requests.get(url,params=payload,headers=headers)
        
        if response.status_code!=200:
            print("Erreur HTTP:",response.status_code)
            print(response.text)
            break
        data=response.json().get("data",{})
        medecins=data.get("data",[])
        if not medecins:
            print("Fin de la pagination")
            break
        for med in medecins:
            Statut=""
            try:
                DocteurdeDocDialy=med.get("is_docdialy_doctor")
                if DocteurdeDocDialy==True:
                    Statut="Actif"
                else:
                    Statut="Fictif"
            except Exception as e:
                Statut="Fictif"
            if Statut in ["Actif","Passif"] :
                identifiant=med.get("id")
                first=med.get("first_name","")or""
                last=med.get("last_name","")or""
                Nom_Complet=med.get("name","").strip()
                specialites=[s["id"]+","+s["name"] for s in med.get("specialities",[])]
                lien=med.get("slug","")
                adresse=med.get("address","")
                email=med.get("email","")or""
                if email:
                    email=email.strip()
                urls={
                    "certificats":f"https://api.docdialy.com/api/website/doctors/{identifiant}/certifications",
                    "languages":f"https://api.docdialy.com/api/website/doctors/{identifiant}/languages",
                    "care_acts":f"https://api.docdialy.com/api/website/doctors/{identifiant}/care-acts",
                    "profile":f"https://api.docdialy.com/api/website/doctors/{identifiant}/profile",
                    "presentation":f"https://api.docdialy.com/api/website/doctors/{identifiant}/schema-doctor"
                    
                }
                def AccederDetails(name_url):
                    name,url =name_url
                    try:
                        response=requests.get(url,headers=headers,timeout=10)
                        if response.status_code==200:
                            return name,response.json()
                        else:
                            return name,{}
                    except Exception as e:
                        return name,{}
                    #ICI J'ai utilisé la methode svt pour gangner du temps et accéeder 
                    #Aux différentes URL en parallèle
                with ThreadPoolExecutor() as executor:
                    results=dict(executor.map(AccederDetails,urls.items()))
                #Certificats et diplomes:
                data_certifs=results.get("certificats",{}).get("data",{}).get("certifications",[])
                label_certificats=[c.get("label")for c in data_certifs if c.get("label")]
                #Langues
                data_langues=results.get("languages",{}).get("data",{}).get("languages",[])
                label_langues=[l.get("label")for l in data_langues if l.get("label")]
                #Actes
                data_actes=results.get("care_acts",{}).get("data",{}).get("care_acts",[])
                label_actes=[a.get("name")for a in data_actes if a.get("name")]
                #Presentation
                data_presentation=results.get("presentation",{}).get("data",{}).get("presentation","")
                Sexe=results.get("profile",{}).get("data",{}).get("gender","")or ""
                if Sexe.lower()=="male":
                    Sexe="Homme"
                elif Sexe.lower()=="female":
                    Sexe="Femme"
                else:
                    Sexe=""
                
                all_medecins.append({
                        "ID Medecin":identifiant,
                        "Civilité":med.get("professional_status").capitalize(),
                        "Statut":Statut,
                        "Prénom":first,
                        "Nom":last,
                        "Nom Complet":Nom_Complet,
                        "Sexe":Sexe,
                        "Télephone":med.get("phone"),
                        "Email":email,
                        "Adresse":adresse,
                        "ID Ville":med.get("city_id",""),
                        "Ville":med.get("city",""),
                        "Spécialités":specialites,
                        "Lien Profil":lien,
                        "Soins et Actes":label_actes,
                        "Langues Parlées":label_langues,
                        "Diplomes et Formations":label_certificats,
                        "Presentation":data_presentation
                        
                    })
                
                print("A","|",adresse,"|",specialites,"|",label_actes,"|",label_certificats,"|",label_langues,"|",data_presentation)
                
        dfDocdialy=pd.DataFrame(all_medecins)
        dfDocdialy.to_excel(r"C:\Users\HP\Desktop\Data_Folder\Fichier_DocDialy_ma_Avant_Nettoyage.xlsx",index=False)        
                
        payload['page']+=1
        time.sleep(1)       
        return dfDocdialy
##############--------------------Fichier Global de Nettoyage--------######################
def nettoyer_dataframe_medecins(DataframeMedecins,plateforme):
    df1=DataframeMedecins
    print("🧾 Colonnes détectées :", df1.columns.tolist())

    # ---------------------------Extraire la Civilité-------------------------------------------------
    if "Nom Complet" in df1.columns:
        civilites = ["Dr", "dr", "Mme", "Mlle", "Pr", "Prof", "professeur"]
        df1["Civilité"] = df1["Nom Complet"].str.strip().str.split().str[0]
        df1["Civilité"] = df1["Civilité"].where(df1["Civilité"].isin(civilites), "")

        PRENOMS_COMPOSES = [
            "Fati Zahra",
            "Fatim Azzahra",
            "Fatima Azzahra",
            "Fatima Ezzahra",
            "Fatima Zahra",
            "Mohammed Amine",
            "Mohamed Amine",
            "Abdel Hakim",
            "Jean Pierre"
        ]
        MOTS_CATEGORIES = ["centre", "cabinet", "laboratoire", "clinique", "center","Laser"]
        MOTS_PARASITES=["Pr","Mme","Mr","Dr","Kiné","Quiétude","Mlle"," Dental","Psychologue"]
        PRENOMS_CONNUS = ["Ouidad", "ahmed", "meryem", "imane", "fatima", "youssef","ahlam"]
        patternMotParasites = r"\b(" + "|".join(MOTS_PARASITES) + r")\b"
        #Nettoyer les mots indésirables avant de séparer le nom complet
        def nettoyer_nom_complet(nom):
            nom = nom.strip()
            if not nom:
                return  "Inconnu"

            # Normalisation des espaces
            nom = re.sub(r"\s+", " ", nom)

            # --- CAS PARTICULIER : "Dr Laboratoire ..." ---
            # Si on trouve Dr suivi d'un mot d'établissement
            if re.search(r"\bDr\s+(Laboratoire|Centre|Cabinet|Clinique|Center)\b", nom, flags=re.IGNORECASE):
                # On garde tout ce qui est après l'établissement
                for mot in MOTS_CATEGORIES:
                    pattern = re.search(rf"\b{mot}\b", nom, flags=re.IGNORECASE)
                    if pattern:
                        # Tout ce qui est après le mot clé
                        nom_medecin = nom[pattern.end():].strip()
                        for prenom in PRENOMS_CONNUS:
                            nom_medecin=re.search(rf"\b{prenom}\b", nom, flags=re.IGNORECASE)
                            if nom_medecin:
                                reste = nom[nom_medecin.start():].strip()
                                reste = re.sub(patternMotParasites, "", reste, flags=re.IGNORECASE).strip()
                                return reste
                        return nom_medecin
            # --- LOGIQUE CLASSIQUE (autres cas) ---
            prefix_match = re.search(r"\b(Dr|Mme|Mlle|Mr|M|Pr)\b", nom, flags=re.IGNORECASE)
            # Vérifier si un mot-clé d’établissement est présent
            for mot in MOTS_CATEGORIES:
                pattern = re.search(rf"\b{mot}\b", nom, flags=re.IGNORECASE)
                if pattern:
                    if prefix_match and prefix_match.start() > pattern.start():
                        nom_medecin = nom[prefix_match.end():].strip()
                        nom_medecin = re.sub(patternMotParasites, "", nom_medecin, flags=re.IGNORECASE).strip()
                        return nom_medecin
                    elif prefix_match and prefix_match.start() < pattern.start():
                        nom_before = nom[:pattern.start()].strip()
                        nom_before = re.sub(patternMotParasites, "", nom_before, flags=re.IGNORECASE).strip()
                        return nom_before
                    else:
                        nom_medecin = nom[pattern.end():].strip()
                        for prenom in PRENOMS_CONNUS:
                        # Supprimer le mot-clé d'établissement et titres
                            match_prenom=re.search(rf"\b{prenom}\b", nom_medecin, flags=re.IGNORECASE)
                            if match_prenom:
                                reste = nom_medecin[match_prenom.start():].strip()
                                reste = re.sub(patternMotParasites, "", reste, flags=re.IGNORECASE).strip()
                                return reste
                        return nom
            # Aucun mot-clé d’établissement
            nom_clean = re.sub(patternMotParasites, "", nom, flags=re.IGNORECASE).strip()
            return nom_clean
        def split_nom_complet(nom_complet):
            if not nom_complet:
                return "", ""
            if pd.isna(nom_complet) or str(nom_complet).strip().lower() in ["nan", "none", ""]:
                return "",""
            nom_complet = str(nom_complet).strip().replace(".", "").title()
            mots = nom_complet.split()
            if len(mots) == 1:
                return mots[0], ""
            titres = ("El", "Al", "Lalla", "Sidi", "Moulay", "Mly","Le")
            prenom = ""
            reste = []
            # Vérifier si le premier mot est un titre
            if mots[0] in titres:
                prenom = mots[0]
                reste = mots[1:]
                for i in range(2, 4):  # tester 2 ou 3 mots
                    if " ".join(reste[:i]) in PRENOMS_COMPOSES:
                        prenom = (prenom + " " + " ".join(reste[:i])).strip()
                        nom = " ".join(reste[i:])
                        return prenom, nom
                if prenom:
                    prenom = (prenom + " " + reste[0]).strip()
                    nom = " ".join(reste[1:])
                    return prenom, nom
            else:
                reste = mots
                for i in range(2, 4):  # tester 2 ou 3 mots
                    if " ".join(reste[:i]) in PRENOMS_COMPOSES:
                        prenom = " ".join(reste[:i]).strip()
                        nom = " ".join(reste[i:])
                        return prenom, nom
        
            return reste[0], " ".join(reste[1:])

        df1["Nom Nettoyé"]=df1["Nom Complet"].apply(lambda x: pd.Series(nettoyer_nom_complet(x)))
        df1[["Prénom","Nom"]]=df1["Nom Nettoyé"].apply(lambda x:pd.Series(split_nom_complet(x)))
    def supprimer_doublons(texte):
        mots = str(texte).split()
        resultat = []
        for mot in mots:
            if not resultat or resultat[-1] != mot:
                resultat.append(mot)
        return " ".join(resultat)
    df1["Prénom"]=df1["Prénom"].astype(str).str.title().str.strip().str.replace("-"," ").apply(supprimer_doublons)
    df1["Nom"]=df1["Nom"].astype(str).str.title().str.strip().str.replace("-"," ").apply(supprimer_doublons)
    #------------------Netoyer Spécialité-----------------------------
    #Prendre uniquement la première Spécialité---------------
    df1["Spécialité"]=df1["Spécialité"].astype(str).apply(lambda x: x.split(",")[0].strip())
    classification = {
        "Anesthésiologie": [
            "réanimateur pédiatrique","réanimateur médical","anesthésie", "anesthésie-réanimation","anesthésiste-réanimateur"
        ],
        "Andrologie": [
            "andrologie","andrologue"
        ],
        "Cardiologie": [
            "cardiologie", "cardiologue", "cardiologie interventionnelle", "rythmologie interventionnelle","rythmologue interventionnel"
        ],
        "Chirurgie": [
            "chirurgie" ,"proctologie","proctologue"
        ],
        "Chirurgie cardiaque": [
            "chirurgie cardio", "chirurgie cardio-vasculaire"
        ],
        "Chirugie esthétique, plastique et reconstructive": [
            "chirurgie esthétique", "chirurgie plastique", "chirurgie réparatrice","médecin esthétique",
            "chirurgie plastique, reconstructrice et esthétique","médecine esthétique","médecine au laser"
        ],
        "Chirurgie générale": [
            "chirurgie générale", "chirurgie general", "chirurgie digestive", "chirurgie cancérologique","Proctologie"
        ],
        "Chirurgie maxillo-faciale": [
            "chirugien cervico-facial","stomatologue","chirurgie maxillo", "chirurgie cervico-faciale", "oto-rhino-laryngologie et chirurgie cervico-faciale","Chirugie Cervico-Faciale","chirurgien cervico-facial"
        ],
        "Chirurgie pédiatrique": [
            "chirurgie pédiatrique"
        ],
        "Chirurgie thoracique": [
            # à compléter si des mots-clés spécifiques apparaissent dans tes données
        ],
        "Chirurgie vasculaire": [
            "phlébologue","chirurgie vasculaire","Phlébologie","angiologue","audioprothésiste"
        ],
        "Neurochirurgie": [
            "neurochirurgie"
        ],
        "Dermatologie": [
            "dermatologue","dermatologie", "dermatologue","dermatologue pédiatrique","dermatologie et venereologie",
            "dermatologie pédiatrique"
        ],
        "Nutrition":["diététique","nutritionniste","nutrition"],
        "Endocrinologie": [
            "diabétologue","diététicien","endocrinologie", "endocrinologue", "endocrinologie et métabolismes",
            "diabétologie nutritionnelle"
        ],
        "Gastro-entérologie": [
            "gastro-entérologue","gastro-entérologue","gastroentérologie", "gastrologie", "gastrologue",
            "hépatogastroentérologie","du pancréas et des voies biliaires", "hepatogastroenterologue","Hépato-Gastro-Entérologie"
        ],
        "Gériatrie": [
            "gérontologue - gériatre","gériatrie", "gerontologie", "la gériatrie","gérontologie"
        ],
        "Gynécologie": [
            "gynécologie", "gynécologue", "gynécologie obstétrique", "gynecologie-obstetrique","gynecologue","perineologue"
        ],
        "Hématologie": [
            "hématologie","hématologue"
        ],
        "Hépatologie":[
            "hépatologie","hépatologue"  # si présent explicitement
        ],
        "Infectiologie": [
            "infectiologie","maladies infectieuses" # à adapter selon tes données
        ],
        "Médecine aiguë": [
            "médecine d'urgence","médecin urgentiste",
        ],
        "Médecine du travail": [
            "médecine du travail","médecin du travail"
        ],
        "Biologie médicale":["laboratoire d'analyses de biologie médicale","biologiste medicale","biologiste (laboratoire d'analyse)","biologie médicale"],
        "Médecine légale":[
            "médecin légiste",
        ],
        "Médecine générale": [
            "médecin généraliste","médecine générale", "omnipraticien", "médecine de famille","généraliste","médecin de famille"
        ],
        "Médecine interne": [
            "médecine interne","médecin interniste","médecin du sommeil","angiologie"
        ],
        "Médecine nucléaire": [
            "médecine nucléaire","médecin nucléaire"
        ],
        "Médecine palliative": [
            "médecine palliative","algologue - traitement de la douleur"
        ],
        "Médecine physique": [
            "ergothérapie","orthophoniste","posturologue","podologue","chiropracteur","Kinesitherapeute","médecine physique", 
            "médecine physique et de réadaptation", "mpr","kinésithérapie","orthophonie","Podologie","kinésithérapeute",
            "kinésithérapeute du sport","médecin physique et de réadaptation","ergothérapeute","médecin physique réadaptateur"
            
        ],
        "Médecine préventive": [
            "médecin du sport","ostéopathe","homéopathe","médecine préventive", "santé publique", "médecine du sport", "médecine aéronautique et spatiale","Naturopathie",
            "Allergologie","Naturopathie","Médecine Chinoise(Mtc)","ostéopathie","homéopathie","acupuncture","allergologue","allergologue-pédiatrique",
            "tabacologue","naturopathe","sophrologue"
            
        ],
        "Néonatologie": [
            "néonatologie"
        ],
        "Néphrologie": [
            "néphrologie","centre de dialyse","néphrologue","néphrologue pédiatrique","neurophysiologiste"
        ],
        "Neurologie": [
            "neurologie","neurologue"
        ],
        "Odontologie": [
            "endodontiste","occlusodontiste","orthodontiste","parodontologiste","pédodontiste",
            "dentiste", "chirurgie dentaire", "chirurgie dentiste", "dentaire","implantologiste",
            "orthodontie", "parodontologie", "pédodontie"
        ],
        "Oncologie": [
            "oncologie", "cancérologie","oncologue - cancérologue","oncologue médical","cancérologue","oncologue"
        ],
        "Obstétrique": [
            "obstétrique", "sage femme","sage-femme"
        ],
        "Ophtalmologie": [
            "ophtalmologie", "orthoptie","ophtalmologue","ophtalmologue pédiatrique","orthoptiste","néonatologiste"
        ],
        "Orthopédie": [
            "traumato-orthopédiste pédiatrique","traumatologue-orthopédiste","Orthopédie traumatolo","orthopédie", "traumatologie", "traumato - orthopedie",
            "chirurgie orthopédiste", "chirurgie traumatologie-orthopédie", "chirurgie orthopédique","Perineologue","Orthopédiste Traumatologue"
        ],
        "Oto-rhino-laryngologie": [
            "orl","oto-rhino-laryngologie","oto-rhino-laryngologie(orl)","oto-rhino-laryngologie et chirurgie cervico-faciale"
        ],
        "Pédiatrie ": [
            "pédiatrie","pédiatre"
        ],
        "Anatomopathologie": [
        "anatomopathologiste", "anatomopathologie", "médecin anatomopathologiste"
        ],
        "Aneumologie ": [
            "pneumologie","pneumologue"
        ],
        "Psychiatrie ": [
            "psychiatrie","psychanalyste","coach développement personnel","hypnothérapeute","pédopsychiatre","psychiatre","psychologue","psychologue","psychomotricien","psychothérapeute","sexologue","sophrologue"
            "psychiatrie","sexologie","neuropsychiatrie", "psychologie", "psychomotricité", "psychothérapie", "pedopsychiatrie","Addictologie","psycologue","addictologue"
        ],
        "Radiologie ": [
            "radiologie","radiologue"
        ],
        "Radiothérapie ": [
            "radiothérapeute","radiothérapie", "oncologie radiothérapie", "oncologie option radiothérapie"
        ],
        "Rhumatologie": [
            "rhumatologie","rhumatologue"
        ],
        "Urologie": [
            "urologie", "chirurgie urologique","urologue","urologue pédiatrique"
        ]}
    def reclasser_specialite(specialite):
        specialite = str(specialite).strip().lower().replace("nan","")
        for categorie, mots_cles in classification.items():
            for mot in mots_cles:
                if mot.lower() in specialite:
                    return categorie
        return specialite if specialite else ""

    df1["Spécialité"]=df1["Spécialité"].apply(reclasser_specialite)
    df1["Spécialité"]=df1["Spécialité"].astype(str).str.title()
    #-------------Nettoyer Ville--------------------------------
    df1["Ville"]=df1["Ville"].astype(str).str.title().replace("none","").replace("NAN","").replace("nan","").replace("Nan","")
    df1["Ville"]=(df1["Ville"]
                .str.replace("Temara","Témara")
                .str.replace("Khemisset","Khémisset")
                .str.replace("Benguérir","Ben Guerir")
                .str.replace("Beni Mellal","Béni Mellal")
                .str.replace("Skhirat","Skhirate")
                .str.replace("Skhiratee","Skhirate")
                .str.replace("Fkih Ben Saleh","Fkih Ben Salah")
                .str.replace("Mohammadia","Mohammédia")
                .str.replace("Mohammedia","Mohammédia")
                .str.replace("Mohammadia","Mohammédia")
                .str.replace("Sefrou","Séfrou")
                .str.replace("Taroudannt","Taroudant")
                .str.replace("Sefrou","Séfrou")
	            .str.replace("Taroudannt","Taroudant")
                .str.replace("Fkih Ben Saleh","Fkih Ben Salah")
                .str.replace("Khemisset","Khémisset")
                .str.replace("Benguérir","Ben Guerir")
                .str.replace("Beni Mellal","Béni Mellal")
                .str.replace("Skhiratee","Skhirate")
                .str.replace("Skhirat","Skhirate")
                .str.replace("Shirate","Skhirate"))
   #------------Nettoyer Telephone-------------------------------
    def detecter_prefix_par_ville(ville):
        ville=str(ville).lower()
        mapping={
            #Maroc
            "casablanca":"+212","rabat":"+212","marrakech":"+212","tanger":"+212",
            #Cameroun
            "douala":"+237","yaoundé":"+237",
            #France
            "paris":"+33","lyon":"+33",
            #Tunisie
            "tunis":"+216",
            #Algérie
            "alger":"+213"
        }
        for ville_cle,prefix in mapping.items():
            if ville_cle in ville:
                return prefix
        return "+212"
    def Nettoyage_phone(tel,ville):
        if pd.isna(tel):
            return ""
            #Suppression des caracteres inutiles
        tel=str(tel).strip().replace("-","").replace(" ","")
        numeros=re.findall(r'\d{7,20}',tel)
        resultats=[]
        prefixe=detecter_prefix_par_ville(ville)
        for n in numeros:
            if n.startswith("212"):
                n="+" +n
            elif n.startswith("0"):
                n=prefixe +n[1:]
            elif n.startswith(("6","5","7"))and len(n)==9:
                n=prefixe +n
            if n.lower() in ["nan","none",""]:
                return ""
            if n not in ["+212","+212nan",""] :
                resultats.append(n)
        return ", ".join(resultats)
    colonnes_telephone={
        "Dabadoc":["Téléphone"],
        "Med":["Téléphone","WhatsApp"],
        "Nabady":["Téléphone","Fix"],
        "Docdialy":["Téléphone"]
    }
    for col in colonnes_telephone.get(plateforme,[]):
        if col in df1.columns:
            df1[col]=df1[[col,"Ville"]].apply(lambda x:Nettoyage_phone(x[col],x["Ville"]),axis=1)
    #--------------Nettoyage des Langues Parlées--------------------------------
    if "Langues Parlées" in df1.columns:
        df1["Langues Parlées"]=(df1["Langues Parlées"]
                            .astype(str).str.replace("عربي","Arabe")
                            .replace("Nan","")
                            .replace("nan","")
                            .replace("NAN","")
                            .replace("none","")
                            .replace("[","")
                            .replace("]",""))
    #-------------------------------------------------------------------------------------------------------------
    
    if plateforme=="Dabadoc": 
        df1=nettoyer_dataframe_medecinsDabadoc(df1,"Dabadoc")
    if plateforme=="Med":
        df1=nettoyer_dataframe_medecinsMed(df1,"Med")
    if plateforme=="Docdialy":
        df1=nettoyer_dataframe_medecinsDocdialy(df1,"Docdialy")
    if plateforme=="Nabady":
        df1=nettoyer_dataframe_medecinsNabady(df1,"Nabady")
    return  df1
#############----------------------Fichier Dabadoc Nettoyage----------#####################
def nettoyer_dataframe_medecinsDabadoc(DataframeMedecins,plateforme):
    df1=DataframeMedecins
    #--------------------------Extraire le dernier Tiret Pour Avoir uniquement le ID sous forme d'un nombre sans serach -----------------------------
    df1["ID Medecin"] = df1["ID Medecin"].str.rsplit("-", n=1).str[-1]
 
#-------------------------------------------------------------------------------------------------------------
    ListString=["ID Medecin","Statut","Civilité","Nom","Prénom"]
    for col in ListString:
        df1[col]=(df1[col].
              astype(str)
              .str.replace("-"," ")
              .replace("nan","")
              .replace("Nan","")
              .replace("NAN","")
              .replace("none","")
              .str.replace("عربي", "Arabe")
              .str.replace(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]+','', regex=True))
    
    Nouvel_Order=["ID Medecin","Statut","Prénom","Nom","Spécialité","Ville","Téléphone","Langues Parlées"]
    df1=df1[Nouvel_Order]
    df1.to_excel(r"C:\Users\HP\Desktop\Data_Folder\TestDabadoc.xlsx",index=False)
    
    return df1
#############----------------------Fichier Med Nettoyage----------#####################
def nettoyer_dataframe_medecinsMed(DataframeMedecins,plateforme):
    df1=DataframeMedecins

# --------------------------------------------Organiser Columns---------------------------------------
    Columns=["ID Medecin","Statut","Civilité","Nom Complet","Téléphone","WhatsApp"]

    for col in Columns:
        df1[col]=df1[col].astype(str).str.replace("nan","").str.replace("None","").str.replace("none","").str.replace("Nan","").str.replace(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]+','', regex=True).str.replace(r"[<>]","",regex=True)
    NouvelOrder=["ID Medecin","Statut","Civilité","Prénom","Nom","Spécialité","Ville","Téléphone","WhatsApp","Langues Parlées"]
    df1=df1[NouvelOrder]
    df1.to_excel(r"C:\Users\HP\Desktop\Data_Folder\TestMed.xlsx",index=False)
    return df1
#############----------------------Fichier Docdialy Nettoyage----------#####################
def nettoyer_dataframe_medecinsDocdialy(DataframeMedecins,plateforme):
    df1=pd.DataFrame(DataframeMedecins)
    df1["Statut"]=df1["Statut"].apply(lambda x:"Actif" if x=="Réel Actif" else "Passif" if x=="Réel Passif" else "Fictif")
    
    ordre = ["ID Medecin","Statut","Civilité","Prénom","Nom","Email"]

    for col in ordre:
        df1[col]=df1[col].fillna("")
        df1[col].astype(str).str.replace("nan","").str.replace("none","").str.replace("NAN","").str.replace("Nan","").replace(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]+','', regex=True)
    Nouvel_ordre = [
        "ID Medecin","Statut","Civilité","Prénom","Nom","Spécialité","Ville","Téléphone","Email"]
    df1=df1[Nouvel_ordre]
    df1.to_excel(r"C:\Users\HP\Desktop\Data_Folder\TestDocdialy.xlsx",index=False)
    return df1
#############----------------------Fichier Nabady Nettoyage----------#####################   
def nettoyer_dataframe_medecinsNabady(DataframeMedecins,plateforme):
    df1=pd.DataFrame(DataframeMedecins)
    #Nettoyer Civilité
    df1["Civilite"]=df1["Civilite"].astype(str).str.replace(".","")
    df1=df1.rename(columns={"Civilite":"Civilité"})
    #Nettoyer Sexe
    df1["Sexe"]=df1["Sexe"].astype(str).str.strip().str.capitalize().replace("none","").replace("NAN","").replace("nan","").replace("Nan","").replace("None","")
    #Nettoyer Date Extraction et Date de Naissance 
   
    df1["Date de Connexion"]=pd.to_datetime(df1["Date de Connexion"],format="%Y-%m-%d %H:%M:%S")
    #Nettoyer les strings
    columns=["ID Medecin","Statut","Civilité","Email","Photo URL"]
    for col in columns:
        df1[col]=df1[col].fillna("")
        df1[col]=df1[col].astype(str).str.replace(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]+','', regex=True).str.replace("none","").str.replace("Nan","").str.replace("nan","").str.replace(r"[-<>]","",regex=True)
    order_columns=["ID Medecin","Statut","Civilité","Prénom","Nom","Sexe","Spécialité","Ville","Téléphone","Fix","Email","Date de Naissance","Langues Parlées","Photo URL","Duree","Date de Connexion"]
    df1=df1[order_columns]
    df1.to_excel(r"C:\Users\HP\Desktop\Data_Folder\TestNabady.xlsx",index=False)
    ListeMedecinsNettoyée=df1
    return ListeMedecinsNettoyée
###############Lancer la methode de Scrapping de dabadoc en filtrant les statuts #######
def lancer_scrapping_dabadoc():   
    flag_path="first__scrappingDabadoc.flag"
    if not os.path.exists(flag_path):
        print("Premier lancement : scraping & nettoyage de tous les status Pour Dabadoc")
        df=get_medecinsDabadoc()
        df=nettoyer_dataframe_medecins(df,"Dabadoc")
        with open(flag_path,"w") as f:
            f.write("done")
    else:
        print("Lancement normal: juste les actifs et les passifs Pour Dabadoc")
        df=get_medecinsDabadoc_actifs_passifs()
        df=nettoyer_dataframe_medecins(df,"Dabadoc")
    return df
##############Lancer la methode de Scrapping de docdialy en filtrant les statuts#######
def lancer_scrapping_docdialy():   
    flag_path="first__scrappingDocdialy.flag"
    if not os.path.exists(flag_path):
        print("Premier lancement : scraping & nettoyage de tous les status Pour Docdialy")
        df=get_medecinsDocdialy()
        df=nettoyer_dataframe_medecins(df,"Docdialy")
        with open(flag_path,"w") as f:
            f.write("done")
    else:
        print("Lancement normal: juste les actifs et les passifs Pour Docdialy aa")
        df=get_medecinsDocdialy_actifs_passifs()
        df=nettoyer_dataframe_medecins(df,"Docdialy")
    return df
#############Lancer la methode de Scrapping de med en filtant les statuts##############
def lancer_scrapping_med():   
    flag_path="first_scrappingMed.flag"
    if not os.path.exists(flag_path):
        print("Premier lancement : scraping & nettoyage de tous les status Pour Med")
        df=get_medecinsMed()
        df=nettoyer_dataframe_medecins(df,"Med")
        with open(flag_path,"w") as f:
            f.write("done")
    else:
        print("Lancement normal: juste les actifs et les passifs Pour Med")
        df=get_medecinsMed_actifs_passifs()
        df=nettoyer_dataframe_medecins(df,"Med")
    return df
#############Lancer la methode de Scrapping de Nabady en filtrant les statuts###########
def lancer_scrapping_nabady():   
    flag_path="first_scrappingNabady.flag"
    if not os.path.exists(flag_path):
        print("Premier lancement : scraping & nettoyage de tous les status Pour Nabady")
        df=get_medecinsNabady()
        df=nettoyer_dataframe_medecins(df,"Nabady")
        with open(flag_path,"w") as f:
            f.write("done")
    else:
        print("Lancement normal: juste les actifs et les passifs Pour Nabady")
        df=get_medecinsNabady_actifs_passifs()
        df=nettoyer_dataframe_medecins(df,"Nabady")
    return df
 ###########----------------unifier_dataframe-----################ 
colonnes_standard=["ID Medecin","Statut","Prénom","Nom","Sexe","Spécialité","Ville","Date de Naissance","Téléphone","WhatsApp","Fix","Email","Duree","Date de Connexion","Date Extraction","Plateforme"]
def unifier_dataframe(df,plateforme):
    df=df.copy()
    df["Plateforme"]=plateforme
    for colonne in colonnes_standard:
        if colonne not in df.columns:
            df[colonne]=""
    return df[colonnes_standard]
##########------Associer des identifiants à quelques indicateurs----------########
def attribuer_ids_df_hash_simple(df, db, collection_name="collection_globale"):
    col = db[collection_name]

    FIELDS   = ["Spécialité", "Ville", "Statut", "Sexe", "Plateforme"]
    PREFIXES = {"Spécialité":"spe", "Ville":"vil", "Statut":"sta", "Sexe":"sex", "Plateforme":"plt"}
    HASH_LEN = 10

    # placeholders stables quand c'est vide
    PLACEHOLDER = {
        "Spécialité": "non renseigné",
        "Ville":      "non renseigné",
        "Statut":     "non renseigné",
        "Sexe":       "non renseigné",
        "Plateforme": "non renseigné",
    }
    # normalisation accent-insensible
    def norm(s):
        if s is None or (isinstance(s, float) and np.isnan(s)):
            return ""
        s = str(s).strip().lower()
        s = unicodedata.normalize("NFD", s)
        return "".join(c for c in s if unicodedata.category(c) != "Mn")

    def extract_libelle(x):
        if isinstance(x, dict):
            return (x.get("libelle") or "").strip()
        return (str(x).strip()) if x is not None else ""

    # 1) charge les IDs existants -> maps[field][lib_norm] = id
    maps = {field: {} for field in FIELDS}
    projection = {f: 1 for f in FIELDS}
    for doc in col.find({}, projection):
        for field in FIELDS:
            val = doc.get(field)
            if isinstance(val, dict):
                lib = (val.get("libelle") or "").strip()
                sid = str(val.get("id") or "").strip()
                if lib and sid:
                    maps[field][norm(lib)] = sid

    # 2) id déterministe (prefix + sha1(f"{field}:{lib_norm}")[:10])
    def make_hash_id(field_name, libelle):
        k = norm(libelle)
        h = hashlib.sha1(f"{field_name}:{k}".encode("utf-8")).hexdigest()[:HASH_LEN]
        return f"{PREFIXES.get(field_name, 'id')}_{h}"

    # 3) value -> {id, libelle} (jamais vide ; vide -> 'non renseigné')
    def to_id_dict(field_name, value):
        lib_raw  = extract_libelle(value)
        lib_text = lib_raw if lib_raw.strip() else PLACEHOLDER[field_name]
        k = norm(lib_text)

        # a) si déjà connu en base -> réutiliser l'id existant (ex: ton sex_… pour "non renseigné")
        sid = maps[field_name].get(k)
        if sid:
            return {"id": sid, "libelle": lib_text}

        # b) si value dict avec id non vide -> mémoriser et garder
        if isinstance(value, dict) and str(value.get("id", "")).strip():
            sid = str(value["id"]).strip()
            maps[field_name][k] = sid
            return {"id": sid, "libelle": lib_text}

        # c) sinon -> générer l'id hashé déterministe
        sid = make_hash_id(field_name, lib_text)
        maps[field_name][k] = sid
        return {"id": sid, "libelle": lib_text}

    # 4) appliquer au DF (structure {id, libelle})
    df_out = df.copy()
    for field in FIELDS:
        if field in df_out.columns:
            df_out[field] = df_out[field].apply(lambda v: to_id_dict(field, v))

    return df_out, maps
##########------------Code de detetion des elements présents,absents et modifiés-------#####################
def detecter_changement_par_plateforme():
    client=MongoClient("mongodb+srv://Sal123:uyoige58@cluster0.vd6q5oj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db=client["BaseMedicale"]
    collection=db["collection_globale"]
    changelog=db["changements_collection_globale"]
    dates=sorted(collection.distinct("Date Extraction"))
    if len(dates)<2:
        print("Pas assez de versions pour comparaison.")
        return pd.DataFrame([])
    date_old=dates[-2]
    date_new=dates[-1]
    print(f"Comparaison entre {date_old} et {date_new}")    
    plateformes=collection.distinct("Plateforme")
    for plateforme in plateformes:
        print(f"Traitement de la plateforme: {plateforme}")
        #Récupérer les docs pour chaque plateforme:
        docs_old=list(collection.find({"Date Extraction":date_old,"Plateforme":plateforme}))
        docs_new=list(collection.find({"Date Extraction":date_new,"Plateforme":plateforme}))
        #Créer un dictionnaire avec un ID Medecin comme clé:
        def statut_norm(doc):
            s = doc.get("Statut","")
            if isinstance(s, dict):
                s = s.get("libelle") or ""
            return str(s).strip().lower()

        allowed = {"actif", "passif"}

        old_by_id = {
    doc["ID Medecin"]: doc
    for doc in docs_old
    if "ID Medecin" in doc and statut_norm(doc) in allowed
        }

        new_by_id = {
    doc["ID Medecin"]: doc
    for doc in docs_new
    if "ID Medecin" in doc and statut_norm(doc) in allowed
    }

        #Detection des changements d'ID:
        ids_old=set(old_by_id.keys())
        ids_new=set(new_by_id.keys())
        #Determinons les ids:
        ids_ajoutes=ids_new-ids_old
        ids_supprimes=ids_old-ids_new
        ids_communs=ids_old & ids_new
        date_interrogation= datetime.now()
        ##+++++Ajouts
        for id in ids_ajoutes:
            changelog.insert_one({
                "type":"ajout",
                "ID Medecin":id,
                "plateforme":plateforme,
                "nouveau":new_by_id[id],
                "date_interrogation":date_interrogation,
                "date_extraction_old":date_old,
                "date_extraction_new":date_new
            })
        ##-----Supprimes
        for id in ids_supprimes:
            changelog.insert_one({
                "oltype":"suppression",
                "ID Medecin":id,
                "plateforme":plateforme,
                "ancien":old_by_id[id],
                "date_interrogation":date_interrogation,
                "date_extraction_old":date_old,
                "date_extraction_new":date_new 
            })
        ## Champs à modifier
        champs_a_verifier=["ID Medecin","Statut","Prénom","Nom","Sexe","Spécialité","Ville","Téléphone","Fix","WhatsApp","Email","Date de Naissance","Langues Parlées","Duree","Date de Connexion","Plateforme"]
        for id in ids_communs:
            old_doc=old_by_id[id]
            new_doc=new_by_id[id]
            differences={}
            for champ in champs_a_verifier:
                if old_doc.get(champ)!=new_doc.get(champ):
                    differences[champ]={
                        "avant":old_doc.get(champ),
                        "aprés":new_doc.get(champ)
                    }
            if differences:
                changelog.insert_one({
                    "type":"modification",
                    "ID Medecin":id,
                    "plateforme":plateforme,
                    "modifications":differences,
                    "date_interrogation":date_interrogation,
                    "date_extraction_old":date_old,
                    "date_extraction_new":date_new
                })
        print(f"Plateforme: {plateforme} -Ajoutes: {len(ids_ajoutes)} -Supprimes : {len(ids_supprimes)} -Modifies : {sum(1 for id in ids_communs if any(old_by_id[id].get(champ)!=new_by_id[id].get(champ) for champ in champs_a_verifier)) }")
    changelog=client["collection_globale"]["changement_collection_globale"]
    changements=list(changelog.find({}))
    stats={}
    for ch in changements:
        plateforme=ch.get("Plateforme","")
        type_changement=ch.get("type","")
        if plateforme not in stats:
            stats[plateforme]={"Ajouts":0,"Suppressions":0,"Modifications":0}
        if type_changement=="ajout":
            stats[plateforme]["Ajouts"]+=1
        elif type_changement=="suppression":
            stats[plateforme]["Suppressions"]+=1
        elif type_changement=="modification":
            stats[plateforme]["Modifications"]+=1
    df_detecter_presence_absence=pd.DataFrame.from_dict(stats,orient="index").reset_index()
    df_detecter_presence_absence=df_detecter_presence_absence.rename(columns={"index":"Plateforme"})
    print("Félicitations la comparaison est terminée entre les deux versions!!!!")
    return df_detecter_presence_absence