import pandas as pd
import os

# --- CONFIGURATION ---
file_travail = r"P:\BiblioTechnique\MOBILITE\_Data\Flux Mobilite\base-flux-mobilite-domicile-lieu-travail-2020.parquet"

def diagnostiquer_probleme_filtrage(filepath):
    if not os.path.exists(filepath):
        print(f"ERREUR : Fichier introuvable.")
        return

    print("--- CHARGEMENT DES DONNÉES ---")
    df = pd.read_parquet(filepath)
    
    # --- SIMULATION DU PROBLÈME ---
    # Voici ce que l'application reçoit souvent de la carte ou du fichier CSV des communes
    test_annemasse_str = "74012"
    test_annemasse_int = 74012
    test_annemasse_space = " 74012 "

    print(f"\n--- DIAGNOSTIC DES COMPARAISONS ---")
    
    # 1. Test avec String propre
    res_str = df[df['code_res'] == test_annemasse_str]
    print(f"1. Recherche avec STRING '{test_annemasse_str}' : {len(res_str)} lignes.")

    # 2. Test avec Integer
    try:
        res_int = df[df['code_res'] == test_annemasse_int]
        print(f"2. Recherche avec INT {test_annemasse_int} : {len(res_int)} lignes.")
    except:
        print(f"2. Recherche avec INT : ERREUR DE TYPE")

    # 3. Test avec Espaces
    res_space = df[df['code_res'] == test_annemasse_space]
    print(f"3. Recherche avec STRING + ESPACES '{test_annemasse_space}' : {len(res_space)} lignes.")

    # --- SIMULATION DE LA LISTE (Comme dans l'appli) ---
    print(f"\n--- SIMULATION DU FILTRAGE DE L'APPLI ---")
    
    # On simule une liste 'communes_in_poly' qui viendrait de gdf_cities
    # Si gdf_cities a lu les codes comme des nombres, on aura ça :
    communes_in_poly_FAUX = [74012, 74015] 
    communes_in_poly_VRAI = ["74012", "74015"]

    filtrage_ko = df[df['code_res'].isin(communes_in_poly_FAUX)]
    filtrage_ok = df[df['code_res'].isin(communes_in_poly_VRAI)]

    print(f"Filtrage avec liste d'ENTIERS : {len(filtrage_ko)} lignes (Si 0, c'est le problème !)")
    print(f"Filtrage avec liste de STRINGS : {len(filtrage_ok)} lignes (Si >0, c'est la solution !)")

    if len(filtrage_ko) == 0 and len(filtrage_ok) > 0:
        print("\nPROBLÈME IDENTIFIÉ : L'application essaie de filtrer avec des NOMBRES alors que le Parquet contient du TEXTE.")
        print("SOLUTION : Il faut forcer 'communes_in_poly' en string dans flux_mobilite_source.py.")

# Exécution
diagnostiquer_probleme_filtrage(file_travail)