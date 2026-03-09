import os
import shutil
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# --- CONFIGURATION ---
# Dossier où tu as téléchargé les 3 fichiers manuellement
SOURCE_DIR = r"P:/BiblioTechnique/MOBILITE/_Data/Base permanente des équipements (BPE)"
# Nom du fichier final
FINAL_FILENAME = "BPE24_France_Enrichie.gpkg"
# On utilise le dossier temporaire de ton ordinateur (souvent sur le SSD C:)
LOCAL_TEMP_PATH = os.path.join(os.path.expanduser("~"), "Desktop", FINAL_FILENAME)

# URL_BPE = "https://www.insee.fr/fr/statistiques/fichier/8217525/BPE24.parquet"
# URL_PASSAGE = "https://www.insee.fr/fr/metadonnees/source/fichier/BPE24_table_passage.csv"
# URL_GAMMES = "https://www.insee.fr/fr/statistiques/fichier/8217535/BPE_gammes_equipements_2024.xlsx"

def prepare_bpe_local_to_network():
    print("--- Démarrage de la consolidation BPE (Optimisée Local) ---")
    
    path_parquet = os.path.join(SOURCE_DIR, "BPE24.parquet")
    path_passage = os.path.join(SOURCE_DIR, "BPE24_table_passage.csv")
    path_gammes = os.path.join(SOURCE_DIR, "BPE_gammes_equipements_2024.xlsx")

    # 1. Lectures (toujours depuis le P:, c'est rapide en lecture)
    print("1/3 Chargement des données sources...")
    df_passage = pd.read_csv(path_passage, sep=';', encoding='utf-8')
    df_gammes = pd.read_excel(path_gammes, sheet_name='Gammes 2024', skiprows=4)
    cols_bpe = ['NOMRS', 'DEPCOM', 'DOM', 'SDOM', 'TYPEQU', 'SIRET', 'LAMBERT_X', 'LAMBERT_Y', 'DCIRIS', 'EPCI']
    df_bpe = pd.read_parquet(path_parquet, columns=cols_bpe)

    # 2. Consolidation
    print("2/3 Jointures et géométrisation...")
    df_enriched = df_bpe.merge(df_passage[['TYPEQU', 'Libelle_TYPEQU', 'Libelle_SDOM', 'Libelle_DOM']], on='TYPEQU', how='left')
    df_enriched = df_enriched.merge(df_gammes[['code équipement', 'gamme']], left_on='TYPEQU', right_on='code équipement', how='left')
    
    geometry = [Point(xy) for xy in zip(df_enriched['LAMBERT_X'], df_enriched['LAMBERT_Y'])]
    gdf = gpd.GeoDataFrame(df_enriched, geometry=geometry, crs="EPSG:2154")

    # 3. Export LOCAL (Sur ton Bureau pour une vitesse max)
    print(f"3/3 Exportation locale (SSD) vers : {LOCAL_TEMP_PATH}...")
    # L'indexation spatiale sera foudroyante en local
    gdf.to_file(LOCAL_TEMP_PATH, driver="GPKG", engine="pyogrio", spatial_index=True)

    # 4. Transfert vers le réseau
    final_network_path = os.path.join(SOURCE_DIR, FINAL_FILENAME)
    print(f"Transfert du fichier vers le lecteur réseau P: ... \n (Peut être long ~ 5min)")
    shutil.move(LOCAL_TEMP_PATH, final_network_path)

    print(f"\nTERMINÉ !")
    print(f"Fichier disponible : {final_network_path}")

if __name__ == "__main__":
    prepare_bpe_local_to_network()