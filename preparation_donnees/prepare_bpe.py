import os
import shutil
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import unicodedata

# --- CONFIGURATION ---
SOURCE_DIR = r"P:/BiblioTechnique/MOBILITE/_Data/Base permanente des équipements (BPE)"
FINAL_FILENAME = "BPE24_France_Enrichie.gpkg"
SCORES_FILENAME = "BPE24_Scores_Communes_France.csv"

LOCAL_TEMP_PATH = os.path.join(os.path.expanduser("~"), "Desktop", FINAL_FILENAME)
LOCAL_TEMP_SCORES_PATH = os.path.join(os.path.expanduser("~"), "Desktop", SCORES_FILENAME)

def normaliser_texte(texte):
    if pd.isna(texte): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texte)) if unicodedata.category(c) != 'Mn').lower().strip()

def prepare_bpe_local_to_network():
    print("--- Démarrage de la consolidation BPE ---")
    
    path_parquet = os.path.join(SOURCE_DIR, "BPE24.parquet")
    path_passage = os.path.join(SOURCE_DIR, "BPE24_table_passage.csv")
    path_gammes = os.path.join(SOURCE_DIR, "BPE_gammes_equipements_2024.xlsx")

    # ==========================================
    # 1. LECTURE ET APLATISSEMENT DES GAMMES
    # ==========================================
    print("1/4 Lecture et préparation du dictionnaire des gammes...")
    df_passage = pd.read_csv(path_passage, sep=';', encoding='utf-8')
    df_gammes = pd.read_excel(path_gammes, sheet_name='Gammes 2024', skiprows=4)
    df_bpe = pd.read_parquet(path_parquet, columns=['NOMRS', 'DEPCOM', 'TYPEQU', 'LAMBERT_X', 'LAMBERT_Y'])

    # On nettoie la colonne TYPEQU de la base brute (suppression des espaces + majuscule forcée)
    df_bpe['TYPEQU'] = df_bpe['TYPEQU'].astype(str).str.strip().str.upper()

    # ASTUCE : On duplique la colonne pour garder la trace du "Code Parent" (ex: AR03)
    df_gammes['code_parent'] = df_gammes['code équipement']
    
    # On aplatit le tableau
    colonnes_codes = ['code équipement', 'regroupement_1', 'regroupement_2', 'regroupement_3']
    df_gammes_flat = df_gammes.melt(
        id_vars=['code_parent', 'gamme'], 
        value_vars=colonnes_codes, 
        value_name='code_final'
    )
    
    # Nettoyage des lignes vides générées par l'aplatissement
    df_gammes_flat = df_gammes_flat.dropna(subset=['code_final'])
    df_gammes_flat['code_final'] = df_gammes_flat['code_final'].astype(str).str.strip().str.upper()
    df_gammes_flat = df_gammes_flat.drop_duplicates(subset=['code_final'])

    # ==========================================
    # 2. JOINTURES
    # ==========================================
    print("2/4 Jointures et géométrisation...")
    # Ajout des Domaines et Sous-domaines
    df_enriched = df_bpe.merge(df_passage[['TYPEQU', 'Libelle_TYPEQU', 'Libelle_SDOM', 'Libelle_DOM']], on='TYPEQU', how='left')
    
    # Ajout des Gammes ET du Code Parent
    df_enriched = df_enriched.merge(
        df_gammes_flat[['code_final', 'gamme', 'code_parent']], 
        left_on='TYPEQU', 
        right_on='code_final', 
        how='left'
    )
    
    # Les équipements non listés par l'INSEE passent en "Hors Gamme"
    df_enriched['gamme'] = df_enriched['gamme'].fillna('Hors Gamme')

    # ==========================================
    # 3. CALCUL DES SCORES
    # ==========================================
    print("3/4 Calcul des scores pour les 35 000 communes de France...")
    df_enriched['gamme_norm'] = df_enriched['gamme'].apply(normaliser_texte)
    
    df_enriched['is_prox'] = df_enriched['gamme_norm'].str.contains('proximite', na=False)
    df_enriched['is_int'] = df_enriched['gamme_norm'].str.contains('intermediaire', na=False)
    df_enriched['is_sup'] = df_enriched['gamme_norm'].str.contains('superieur', na=False)

    synthese_list = []
    for code_insee, group in df_enriched.groupby('DEPCOM'):
        
        # LA CORRECTION EST ICI : On compte les "code_parent" uniques, pas les "TYPEQU"
        prox_presents = group[group['is_prox']]['code_parent'].nunique()
        int_presents = group[group['is_int']]['code_parent'].nunique()
        sup_presents = group[group['is_sup']]['code_parent'].nunique()
        
        score_prox = (prox_presents / 26) * 100
        score_int = (int_presents / 45) * 100
        score_sup = (sup_presents / 59) * 100
        
        if score_sup > 50 and score_int > 50: echelon = "Pôle Supérieur"
        elif score_int > 50 and score_prox > 50: echelon = "Pôle Intermédiaire"
        elif score_prox > 50: echelon = "Pôle de Proximité"
        else: echelon = "Commune non-pôle"
            
        synthese_list.append({
            'code_insee': code_insee,
            'Score proximité': round(score_prox, 1),
            'Score intermédiaire': round(score_int, 1),
            'Score supérieur': round(score_sup, 1),
            'Echelon': echelon
        })
    
    df_scores = pd.DataFrame(synthese_list)
    df_scores.to_csv(LOCAL_TEMP_SCORES_PATH, index=False, sep=';', encoding='utf-8')

    # ==========================================
    # 4. EXPORT GÉOGRAPHIQUE
    # ==========================================
    print("4/4 Création du GeoPackage spatial...")
    geometry = [Point(xy) for xy in zip(df_enriched['LAMBERT_X'], df_enriched['LAMBERT_Y'])]
    
    # Nettoyage des colonnes techniques avant l'export
    colonnes_a_supprimer = ['gamme_norm', 'is_prox', 'is_int', 'is_sup', 'code_final', 'code_parent']
    df_export = df_enriched.drop(columns=colonnes_a_supprimer)
    
    gdf = gpd.GeoDataFrame(df_export, geometry=geometry, crs="EPSG:2154")
    gdf.to_file(LOCAL_TEMP_PATH, driver="GPKG", engine="pyogrio", spatial_index=True)

    # ==========================================
    # TRANSFERT VERS LE RÉSEAU
    # ==========================================
    final_network_path = os.path.join(SOURCE_DIR, FINAL_FILENAME)
    final_network_scores_path = os.path.join(SOURCE_DIR, SCORES_FILENAME)
    
    print(f"Transfert des fichiers vers le lecteur réseau P: ... (~ 2 à 5min)")
    shutil.move(LOCAL_TEMP_PATH, final_network_path)
    shutil.move(LOCAL_TEMP_SCORES_PATH, final_network_scores_path)

    print(f"\nTERMINÉ ! Base nationale à jour.")

if __name__ == "__main__":
    prepare_bpe_local_to_network()