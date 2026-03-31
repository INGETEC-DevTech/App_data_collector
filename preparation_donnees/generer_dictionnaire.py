import os
import json
import requests

def executer_mise_a_jour():
    print("Téléchargement du dictionnaire des territoires (avec BBox) depuis geo.api.gouv.fr...")
    dico_final = {"Commune": {}, "EPCI": {}}

    try:
        # 1. Ajout de 'bbox' dans l'URL pour les Communes
        resp_c = requests.get("https://geo.api.gouv.fr/communes?fields=nom,code,centre,bbox&format=json", timeout=15)
        resp_c.raise_for_status()
        for c in resp_c.json():
            if 'centre' in c and 'bbox' in c: # On vérifie qu'on a bien la bbox
                dico_final["Commune"][c['nom']] = {
                    "code": c['code'],
                    "centre": c['centre']['coordinates'],
                    "bbox": c['bbox'] # Contient [lonMin, latMin, lonMax, latMax]
                }

        # 2. Ajout de 'bbox' dans l'URL pour les EPCI
        resp_e = requests.get("https://geo.api.gouv.fr/epcis?fields=nom,code,centre,bbox&format=json", timeout=15)
        resp_e.raise_for_status()
        for e in resp_e.json():
            if 'centre' in e and 'bbox' in e:
                dico_final["EPCI"][e['nom']] = {
                    "code": e['code'],
                    "centre": e['centre']['coordinates'],
                    "bbox": e['bbox']
                }

        assets_dir = os.path.join(os.path.dirname(__file__), "..", "assets")
        os.makedirs(assets_dir, exist_ok=True)
        chemin_fichier = os.path.join(assets_dir, "territoires_dico.json")
        with open(chemin_fichier, "w", encoding="utf-8") as f:
            json.dump(dico_final, f, ensure_ascii=False)

        print(f"-> SUCCÈS : Dictionnaire généré ! ({len(dico_final['Commune'])} communes)")

    except Exception as e:
        print(f"-> ERREUR lors de la génération du dictionnaire : {e}")

if __name__ == "__main__":
    executer_mise_a_jour()