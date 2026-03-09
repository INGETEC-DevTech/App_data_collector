# mobilité_datacollector/data_sources/bnlc_source.py

import os
import tempfile
import requests
import pandas as pd
import geopandas as gpd
from pyproj import Transformer
from data_sources.base_source import SourceDeDonneesBase
from shapely.ops import transform
import pyproj

class BnlcSource(SourceDeDonneesBase):
    def __init__(self, config: dict):
        super().__init__(config)

    @property
    def supports_update(self) -> bool:
        return True

    def valider_lien(self):
        url = self.config.get("api_config", {}).get("csv_url")
        return (True, "URL API BNLC OK") if url else (False, "URL manquante")

    def get_parametres_specifiques_ui(self):
        return None

    def formater_options_collecte(self, valeurs_ui) -> dict:
        return {}

    def collecter_donnees(self, dossier_export_local, perimetre_selection_objet, options_specifiques):
        log_callback = options_specifiques.get("log_callback", print)
        progress_callback = options_specifiques.get("progress_callback")

        try:
            if progress_callback: progress_callback(10, 100)
            
            url = self.config["api_config"]["csv_url"]
            bbox_coords = perimetre_selection_objet["value"]
            source_crs = perimetre_selection_objet.get("crs", "EPSG:2154")

            # 1. Préparation du filtre spatial
            transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
            minx, miny = transformer.transform(bbox_coords[0], bbox_coords[1])
            maxx, maxy = transformer.transform(bbox_coords[2], bbox_coords[3])

            # 2. Téléchargement
            log_callback("Démarrage de la collecte : Lieux de covoiturage (BNLC)")
            log_callback("  > [Action] : Téléchargement du fichier national...")
            temp_csv = os.path.join(tempfile.gettempdir(), "bnlc_national.csv")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            with open(temp_csv, 'wb') as f:
                f.write(response.content)

            log_callback("  > [Action] : Lecture et conversion des données...")
            if progress_callback:
                progress_callback(50, 100) # Le téléchargement est fait, on passe au traitement

            # 3. Lecture avec détection de colonnes sécurisée
            # On force la lecture de toutes les colonnes en texte d'abord pour éviter les erreurs de type
            df = pd.read_csv(temp_csv, sep=None, engine='python', dtype=str)
            
            # Noms de colonnes standards dans le schéma BNLC
            # Priorité aux noms officiels : 'Xlong' et 'Ylat'
            lon_col = next((c for c in df.columns if c.lower() in ['xlong', 'longitude', 'lon']), None)
            lat_col = next((c for c in df.columns if c.lower() in ['ylat', 'latitude', 'lat']), None)

            if not lon_col or not lat_col:
                return False, f"Colonnes de coordonnées introuvables. Colonnes dispo: {list(df.columns[:5])}"

            # Conversion explicite en numérique, les erreurs deviennent des NaN
            df[lon_col] = pd.to_numeric(df[lon_col], errors='coerce')
            df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
            
            # Suppression des lignes où les coordonnées sont invalides
            df = df.dropna(subset=[lon_col, lat_col])

            # Création du GeoDataFrame
            gdf = gpd.GeoDataFrame(
                df, 
                geometry=gpd.points_from_xy(df[lon_col], df[lat_col]), 
                crs="EPSG:4326"
            )

            # 4. Filtrage spatial
            log_callback("Filtrage de la zone d'étude...")
            gdf_clipped = gdf.cx[minx:maxx, miny:maxy].copy()

            if gdf_clipped.empty:
                return True, "Aucun lieu de covoiturage trouvé dans cette zone."

            # 5. Export
            output_crs = "EPSG:2154"
            gdf_clipped = gdf_clipped.to_crs(output_crs)
            log_callback(f"  > [Filtrage] : {len(gdf_clipped)} lieux trouvés dans le périmètre.")
            
            dest_folder = os.path.join(dossier_export_local, self.config.get("export_subdirectory", "COVOITURAGE"))
            os.makedirs(dest_folder, exist_ok=True)
            
            path_out = os.path.join(dest_folder, "covoiturage_bnlc.gpkg")
            
            # --- FILTRAGE PAR POLYGONE ---
            mask_2154 = perimetre_selection_objet.get("polygon")
            if mask_2154 is not None and not gdf_clipped.empty:
                # Le masque est déjà normalisé en Lambert 93
                gdf_clipped = gdf_clipped[gdf_clipped.geometry.within(mask_2154)].copy()

            gdf_clipped.to_file(path_out, driver="GPKG", engine="pyogrio")

            if os.path.exists(temp_csv): os.remove(temp_csv)

            if progress_callback: progress_callback(100, 100)
            return True, f"Succès : {len(gdf_clipped)} lieux de covoiturage récupérés."

        except Exception as e:
            return False, f"Erreur lors de la collecte BNLC : {str(e)}"