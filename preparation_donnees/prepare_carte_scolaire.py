import os
import pandas as pd
import geopandas as gpd
from core.logger_config import logger

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
        
        cols_points = ['Identifiant_de_l_etablissement', 'Nom_etablissement', 'Nom_commune', 'Code_postal', 'geometry']
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
        logger.info("Calcul des affectations communales (Concaténation)...")
        
        # On récupère la liste unique des liens Commune <-> Collège
        noms_colleges = df_rues[['code_insee', 'code_rne']].drop_duplicates()
        
        # On y ajoute le nom du collège ET la commune du collège depuis l'annuaire
        noms_colleges = noms_colleges.merge(
            df_cible[['Identifiant_de_l_etablissement', 'Nom_etablissement', 'Nom_commune']], 
            left_on='code_rne', 
            right_on='Identifiant_de_l_etablissement', 
            how='left'
        )

        # Fonction pour joindre les textes avec le séparateur " | "
        def join_strings(series):
            return " | ".join(series.fillna("Non renseigné").astype(str))

        # On groupe par code INSEE et on aggrège tout d'un coup
        statut_communes = noms_colleges.groupby('code_insee').agg(
            Nombre_de_colleges=('code_rne', 'nunique'),
            Nom_College=('Nom_etablissement', join_strings),
            Code_College=('code_rne', join_strings),
            Commune_College=('Nom_commune', join_strings)
        ).reset_index()

        # On renomme proprement pour l'export final
        statut_communes = statut_communes.rename(columns={
            'Nombre_de_colleges': 'Nombre de collèges',
            'Nom_College': 'Nom Collège',
            'Code_College': 'Code Collège',
            'Commune_College': 'Commune Collège'
        })
        
        # Sauvegarde
        statut_communes.to_csv(path_csv_statuts, sep=';', index=False, encoding='utf-8-sig')
        
        # Nettoyage
        logger.info("Nettoyage des fichiers sources bruts...")
        os.remove(fichier_parquet)
        os.remove(fichier_csv)

        logger.info("Mise à jour de la Carte Scolaire terminée avec succès !")

    except Exception as e:
        logger.error(f"Erreur lors de la préparation de la Carte Scolaire : {e}")
        raise RuntimeError(f"Échec de la préparation de la carte scolaire: {e}")