import os
import pandas as pd
import geopandas as gpd
from logger_config import logger

# --- DOSSIERS CIBLES SUR LE LECTEUR P:/ ---
CARTE_SCOLAIRE_DIR = r"P:\BiblioTechnique\MOBILITE\_Data\Carte Scolaire"
TARGET_GPKG = "carte_scolaire_points.gpkg"
TARGET_CSV_RUES = "dictionnaire_rues.csv"
TARGET_CSV_STATUTS = "statut_communes.csv"

def executer_mise_a_jour(fichier_parquet, fichier_csv):
    try:
        logger.info("--- Démarrage de la préparation de la Carte Scolaire ---")
        os.makedirs(CARTE_SCOLAIRE_DIR, exist_ok=True)
        
        path_gpkg = os.path.join(CARTE_SCOLAIRE_DIR, TARGET_GPKG)
        path_csv_rues = os.path.join(CARTE_SCOLAIRE_DIR, TARGET_CSV_RUES)
        path_csv_statuts = os.path.join(CARTE_SCOLAIRE_DIR, TARGET_CSV_STATUTS)

        # 1. LECTURE DES FICHIERS BRUTS
        logger.info("1/4 Lecture des rues et secteurs (Parquet)...")
        df_rues = pd.read_parquet(fichier_parquet)
        
        logger.info("2/4 Lecture de l'annuaire (CSV)...")
        df_annuaire = pd.read_csv(fichier_csv, sep=';', dtype=str, on_bad_lines='skip')
        
        # --- EXPORT 1 : POINTS DES COLLÈGES ---
        logger.info("3/4 Création de la couche des Collèges (Points)...")
        codes_colleges = df_rues['code_rne'].dropna().unique()
        df_cible = df_annuaire[df_annuaire['Identifiant_de_l_etablissement'].isin(codes_colleges)].copy()
        df_cible['longitude'] = pd.to_numeric(df_cible['longitude'], errors='coerce')
        df_cible['latitude'] = pd.to_numeric(df_cible['latitude'], errors='coerce')
        df_cible = df_cible.dropna(subset=['longitude', 'latitude'])
        
        gdf_points = gpd.GeoDataFrame(
            df_cible, 
            geometry=gpd.points_from_xy(df_cible.longitude, df_cible.latitude),
            crs="EPSG:4326"
        )
        
        cols_points = ['Identifiant_de_l_etablissement', 'Nom_etablissement', 'Nom_commune', 'Code_postal', 'Adresse_1', 'Telephone', 'Mail', 'geometry']
        gdf_points = gdf_points[[c for c in cols_points if c in gdf_points.columns]]
        gdf_points = gdf_points.rename(columns={'Identifiant_de_l_etablissement': 'code_rne'})
        
        logger.info("Export des points vers le GeoPackage...")
        gdf_points.to_file(path_gpkg, layer="colleges_points", driver="GPKG", spatial_index=True)

        # --- EXPORT 2 : LE DICTIONNAIRE DES RUES ---
        logger.info("4/4 Préparation des dictionnaires (Rues et Statuts Communaux)...")
        cols_rues = ['code_insee', 'libelle_commune', 'type_et_libelle', 'n_de_voie_debut', 'n_de_voie_fin', 'parite', 'code_rne']
        df_rues_export = df_rues[[c for c in cols_rues if c in df_rues.columns]].copy()
        
        df_rues_export = df_rues_export.merge(
            df_cible[['Identifiant_de_l_etablissement', 'Nom_etablissement']], 
            left_on='code_rne', 
            right_on='Identifiant_de_l_etablissement', 
            how='left'
        ).drop(columns=['Identifiant_de_l_etablissement'])
        
        df_rues_export.to_csv(path_csv_rues, sep=';', index=False, encoding='utf-8-sig')

        # --- EXPORT 3 : LE STATUT DES COMMUNES ---
        logger.info("Calcul des statuts communaux (Vectorisation)...")
        # On calcule le nombre de collèges par commune
        statut_communes = df_rues.groupby('code_insee')['code_rne'].nunique().reset_index()
        statut_communes.columns = ['code_insee', 'nb_colleges']
        
        noms_colleges = df_rues[['code_insee', 'code_rne']].drop_duplicates()
        noms_colleges = noms_colleges.merge(df_cible[['Identifiant_de_l_etablissement', 'Nom_etablissement']], left_on='code_rne', right_on='Identifiant_de_l_etablissement', how='left')

        # OPTIMISATION : Jointure globale (Prend 0.1 seconde au lieu de 15 minutes)
        noms_uniques = noms_colleges.drop_duplicates(subset=['code_insee'])
        statut_communes = statut_communes.merge(noms_uniques[['code_insee', 'Nom_etablissement']], on='code_insee', how='left')

        # OPTIMISATION : Application des statuts d'un seul coup (Vectorisation)
        # Cas 1 : Commune avec 1 seul collège
        statut_communes.loc[statut_communes['nb_colleges'] == 1, 'Statut_Carte_Scolaire'] = "Unique : " + statut_communes['Nom_etablissement'].fillna("Inconnu")
        # Cas 2 : Commune avec plusieurs collèges
        statut_communes.loc[statut_communes['nb_colleges'] > 1, 'Statut_Carte_Scolaire'] = "Partagée (" + statut_communes['nb_colleges'].astype(str) + " collèges)"
        
        # Sauvegarde
        statut_communes[['code_insee', 'Statut_Carte_Scolaire']].to_csv(path_csv_statuts, sep=';', index=False, encoding='utf-8-sig')
        
        # Nettoyage
        logger.info("Nettoyage des fichiers sources bruts...")
        os.remove(fichier_parquet)
        os.remove(fichier_csv)

        logger.info("Mise à jour de la Carte Scolaire terminée avec succès !")

    except Exception as e:
        logger.error(f"Erreur lors de la préparation de la Carte Scolaire : {e}")
        raise RuntimeError(f"Échec de la préparation de la carte scolaire: {e}")