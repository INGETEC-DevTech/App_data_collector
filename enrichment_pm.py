# enrichment_pm.py

import os
import pandas as pd
import geopandas as gpd
import time

def get_pm_csv_filepath(pm_config, dept_code_from_parcel_idu):
    """
    Construit le nom de fichier CSV PM basé sur le code département extrait de l'IDU parcelle.
    Ex: '01' -> PM_24_NB_010.csv
        '69' -> PM_24_NB_690.csv
        '2A' -> PM_24_NB_2A0.csv
        '971' (issu d'un IDU commençant par '97' et commune '1xx') -> PM_24_NB_971.csv
    """
    prefix = pm_config.get("csv_file_prefix", "PM_24_NB_")
    extension = pm_config.get("csv_file_extension", ".csv")
    
    # La logique de nommage des fichiers CSV que tu as donnée:
    # Si le code département est 01 alors le fichier sera _010.csv
    # Si le code est 69 alors le fichier sera _690.csv
    # si le code est 971 alors _971.csv
    # Donc, si 2 caractères (métropole/Corse), on ajoute "0". Si 3 caractères (DOM), on garde tel quel.
    
    # Le dept_code_from_parcel_idu est sur 2 caractères (ex: "01", "69", "2A", "97")
    # ou 3 caractères si c'est un DOM que nous aurions déjà identifié (mais tu as dit 2 chars pour l'IDU parcelle)
    # Basé sur ta dernière clarification: "Le département est toujours sur deux chiffres [dans l'IDU parcelle WFS]"
    # et pour les CSV: "Si le code département est 01 alors le fichier sera _010.csv", "si le code est 971 alors _971.csv"
    # Cela implique que pour trouver le CSV pour un IDU "97...", il faut le code DOM complet.
    # Pour l'instant, on va supposer que dept_code_from_parcel_idu est le code à utiliser dans le nom de fichier,
    # et que ta fonction qui l'extrait de l'IDU gère déjà la distinction DOM/métro pour le nom de fichier.
    # Ou, plus simple : on construit le suffixe basé sur les règles pour les CSV.

    suffix_csv = ""
    if dept_code_from_parcel_idu.startswith("97") and len(dept_code_from_parcel_idu) == 3: # ex: "971"
        suffix_csv = dept_code_from_parcel_idu
    elif len(dept_code_from_parcel_idu) == 2: # ex: "01", "69", "2A"
        suffix_csv = dept_code_from_parcel_idu + "0"
    else: # Cas non prévu ou code département déjà au format trigramme ?
        suffix_csv = dept_code_from_parcel_idu # Fallback
        print(f"AVERTISSEMENT (enrichment_pm.py): Format de code département inattendu pour nom de fichier CSV: {dept_code_from_parcel_idu}")

    filename = f"{prefix}{suffix_csv}{extension}"
    return os.path.join(pm_config["csv_directory_path"], filename)


def build_pm_join_idu(row, pm_config):
    """Construit l'IDU de 14 caractères pour la jointure à partir d'une ligne du CSV PM."""
    try:
        csv_cols = pm_config["csv_columns"]
        target_lengths = pm_config["idu_target_lengths"]

        # 1. Département (depuis CSV, normalisé à 2 caractères pour l'IDU)
        dep_csv_raw = str(row.get(csv_cols['departement'], '')).strip()
        dep_final_for_idu = ""
        if len(dep_csv_raw) == 1: # ex: "1"
            dep_final_for_idu = "0" + dep_csv_raw
        elif len(dep_csv_raw) == 3 and dep_csv_raw.startswith("97"): # ex: "971"
            dep_final_for_idu = dep_csv_raw[:2] # On garde "97"
        elif len(dep_csv_raw) == 2: # ex: "01", "69", "2A"
            dep_final_for_idu = dep_csv_raw
        else: # Format inattendu
            return None 
        
        # Vérifier si la longueur cible est bien 2
        if len(dep_final_for_idu) != target_lengths['departement']:
             # print(f"Avertissement: longueur dep_final_for_idu ({dep_final_for_idu}) != cible ({target_lengths['departement']})")
             # On pourrait décider de ne pas padder ici si la normalisation est censée donner la bonne longueur.
             # Pour l'instant, on se fie à la normalisation ci-dessus.
             pass


        com_csv = str(row.get(csv_cols['code_commune'], '')).strip()
        sec_csv = str(row.get(csv_cols['section'], '')).strip().upper() # Sections en majuscules
        pla_csv = str(row.get(csv_cols['no_plan'], '')).strip()

        com_padded = com_csv.zfill(target_lengths['code_commune'])
        sec_padded = sec_csv.zfill(target_lengths['section']) # Section sur 2 caractères
        pla_padded = pla_csv.zfill(target_lengths['no_plan'])
        prefix_section = target_lengths['section_prefix'] # "000"

        # Format final: DPT(2) + COM(3) + "000" + SECTION(2) + PLAN(4)
        idu_join = f"{dep_final_for_idu}{com_padded}{prefix_section}{sec_padded}{pla_padded}"
        
        if len(idu_join) != 14: # Vérification de la longueur finale
            # print(f"Avertissement: IDU PM généré de longueur incorrecte ({len(idu_join)} au lieu de 14): {idu_join} pour ligne {row}")
            return None
        return idu_join
    except Exception as e:
        # print(f"Erreur construction IDU PM pour ligne {row}: {e}") # Peut être verbeux
        return None


def enrich_parcels_with_pm_data(parcel_gpkg_path: str, parcel_layer_name: str, pm_config: dict, log_callback=print) -> bool:
    """
    Enrichit un fichier GeoPackage de parcelles avec les données des propriétaires Personnes Morales.
    """
    pm_csv_dir = pm_config.get("csv_directory_path")
    
    if not pm_csv_dir or not os.path.isdir(pm_csv_dir):
        log_callback(f"Avertissement: Dossier des CSV Personnes Morales non configuré ou invalide. Enrichissement annulé.")
        return True

    log_callback("  Début de l'enrichissement avec les données Personnes Morales...")
    t_start_enrich = time.perf_counter()

    try:
        gdf_parcelles = gpd.read_file(parcel_gpkg_path, layer=parcel_layer_name)
        if gdf_parcelles.empty:
            log_callback("  Aucune parcelle à enrichir dans le fichier GPKG."); return True
        
        # MODIFICATION : Nom de fichier court
        log_callback(f"  {len(gdf_parcelles)} parcelles lues depuis {os.path.basename(parcel_gpkg_path)} (couche: {parcel_layer_name}).")

        col_idu_parcelles = None
        potential_idu_names = ['id', 'idu', 'IDU', 'IDUPRO', 'IDU_PARCEL']
        for name in potential_idu_names:
            if name in gdf_parcelles.columns:
                col_idu_parcelles = name
                break
        
        if not col_idu_parcelles:
            log_callback(f"  ERREUR: Colonne IDU non trouvée dans le GPKG.") ; return False
        
        log_callback(f"  Utilisation de la colonne '{col_idu_parcelles}' comme IDU.")
        gdf_parcelles['join_key_parcelles'] = gdf_parcelles[col_idu_parcelles].astype(str).str.strip().str.upper()

        def extract_dep_code_from_parcel_idu(idu_str):
            if isinstance(idu_str, str) and len(idu_str) >= 2:
                dep_part = idu_str[:2]
                if dep_part == "97" and len(idu_str) >= 5:
                    com_part = idu_str[2:5]
                    return dep_part + com_part[0]
                return dep_part
            return None

        departements_a_chercher_csv = gdf_parcelles['join_key_parcelles'].apply(extract_dep_code_from_parcel_idu).unique()
        departements_a_chercher_csv = [dep for dep in departements_a_chercher_csv if dep]
        log_callback(f"  Codes départements identifiés : {departements_a_chercher_csv}")

        all_pm_data_list = []
        for dept_code_for_csv_suffix in departements_a_chercher_csv:
            actual_csv_suffix_part = dept_code_for_csv_suffix + "0" if len(dept_code_for_csv_suffix) == 2 else dept_code_for_csv_suffix
            
            nom_fichier_csv = f"{pm_config['csv_file_prefix']}{actual_csv_suffix_part}{pm_config['csv_file_extension']}"
            chemin_csv = os.path.join(pm_csv_dir, nom_fichier_csv)
            
            if os.path.exists(chemin_csv):
                # MODIFICATION : Un seul log clair avec le nom court
                log_callback(f"    Lecture des données propriétaires : {nom_fichier_csv}")
                try:
                    df_pm_dep = pd.read_csv(chemin_csv, encoding=pm_config['csv_encoding'], dtype=str, sep=';', low_memory=False)
                    df_pm_dep['idu_join_pm'] = df_pm_dep.apply(build_pm_join_idu, axis=1, pm_config=pm_config)
                    df_pm_dep.dropna(subset=['idu_join_pm'], inplace=True)
                    
                    # ... (le reste du traitement des colonnes reste identique) ...
                    cols_to_extract_renamed = {'idu_join_pm': 'idu_join_pm'}
                    for out_col_key, csv_col_key in pm_config["data_columns_from_csv"].items():
                        output_col_name = pm_config["output_column_names"][out_col_key]
                        csv_col_name = pm_config["csv_columns"][csv_col_key]
                        if csv_col_name in df_pm_dep.columns:
                            cols_to_extract_renamed[output_col_name] = csv_col_name
                        else:
                            df_pm_dep[csv_col_name] = None

                    df_pm_dep_selection = pd.DataFrame()
                    for final_name, original_name_or_self in cols_to_extract_renamed.items():
                        if original_name_or_self == 'idu_join_pm':
                             df_pm_dep_selection[final_name] = df_pm_dep['idu_join_pm']
                        elif original_name_or_self in df_pm_dep.columns :
                             df_pm_dep_selection[final_name] = df_pm_dep[original_name_or_self]

                    if not df_pm_dep_selection.empty and 'idu_join_pm' in df_pm_dep_selection.columns:
                        all_pm_data_list.append(df_pm_dep_selection)

                except Exception as e_csv:
                    log_callback(f"    Erreur sur le fichier {nom_fichier_csv}: {e_csv}")
            else:
                log_callback(f"    Avertissement: Fichier {nom_fichier_csv} non trouvé.")

        if not all_pm_data_list:
            log_callback("  Aucune donnée Personne Morale trouvée."); return True
        
        log_callback(f"  Concaténation des données de {len(all_pm_data_list)} fichier(s) CSV...")
        df_all_pm = pd.concat(all_pm_data_list, ignore_index=True)
        df_all_pm.drop_duplicates(subset=['idu_join_pm'], keep='first', inplace=True)
        
        log_callback(f"  Jointure des parcelles avec {len(df_all_pm)} entrées propriétaires...")
        t_join_start = time.perf_counter()
        gdf_parcelles_enrichies = gdf_parcelles.merge(df_all_pm, left_on='join_key_parcelles', right_on='idu_join_pm', how='left')
        gdf_parcelles_enrichies.drop(columns=['join_key_parcelles', 'idu_join_pm'], inplace=True, errors='ignore')
        
        nb_joins = gdf_parcelles_enrichies[pm_config["output_column_names"]["denomination"]].notna().sum()
        log_callback(f"  Jointure terminée : {nb_joins} parcelles enrichies.")

        # MODIFICATION : Nom court pour la sauvegarde
        log_callback(f"  Mise à jour du fichier : {os.path.basename(parcel_gpkg_path)}")
        gdf_parcelles_enrichies.to_file(parcel_gpkg_path, driver="GPKG", layer=parcel_layer_name)
        
        log_callback(f"  Enrichissement terminé en {time.perf_counter() - t_start_enrich:.2f} sec.")
        return True

    except Exception as e_enrich:
        log_callback(f"  ERREUR MAJEURE lors de l'enrichissement : {e_enrich}")
        return False