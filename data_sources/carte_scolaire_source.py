import os
import requests
import geopandas as gpd
import pandas as pd
from data_sources.base_source import SourceDeDonneesBase
from pyproj import Transformer
from logger_config import logger

class CarteScolaireSource(SourceDeDonneesBase):
    def __init__(self, config: dict):
        super().__init__(config)
        file_conf = self.config.get("local_file_config", {})
        self.path_gpkg = file_conf.get("path")
        self.path_csv_rues = file_conf.get("path_csv_rues")
        self.path_csv_statuts = file_conf.get("path_csv_statuts")
        self.native_crs = file_conf.get("native_crs", "EPSG:4326")

        # Fini le fichier local ! On n'a plus besoin du path_communes_hd.

    @property
    def supports_update(self) -> bool:
        return True

    def valider_lien(self):
        if self.path_gpkg and os.path.exists(self.path_gpkg) and self.path_csv_statuts and os.path.exists(self.path_csv_statuts):
            return True, "Fichiers Carte Scolaire prêts sur P:/."
        return False, "Fichiers Carte Scolaire introuvables sur le réseau."

    def get_parametres_specifiques_ui(self):
        return None

    def formater_options_collecte(self, valeurs_ui) -> dict:
        return {}

    def collecter_donnees(self, dossier_export_local, perimetre_selection_objet, options_specifiques):
        prog = options_specifiques.get("progress_callback")

        try:
            # 1. Filtre Spatial (Bounding Box)
            bbox_coords = perimetre_selection_objet["value"] 
            source_crs = perimetre_selection_objet.get("crs", "EPSG:2154")
            
            # BBOX en EPSG:4326 pour lire les points GPKG locaux
            transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
            minx_4326, miny_4326 = transformer.transform(bbox_coords[0], bbox_coords[1])
            maxx_4326, maxy_4326 = transformer.transform(bbox_coords[2], bbox_coords[3])
            filter_bbox_4326 = (minx_4326, miny_4326, maxx_4326, maxy_4326)

            if prog: prog(10, 100)
            
            dest_folder = os.path.join(dossier_export_local, self.config.get("export_subdirectory", "CARTE_SCOLAIRE"))
            os.makedirs(dest_folder, exist_ok=True)
            mask_2154 = perimetre_selection_objet.get("polygon")
            
            # 2. Extraction Couche 1 : Points des Collèges (Fichier Local)
            logger.debug("Extraction des Points Collèges...")
            gdf_points = gpd.read_file(self.path_gpkg, layer="colleges_points", bbox=filter_bbox_4326, engine="pyogrio")
            gdf_points = gdf_points.to_crs("EPSG:2154")
            
            if mask_2154 is not None and not gdf_points.empty:
                gdf_points = gdf_points[gdf_points.geometry.intersects(mask_2154)].copy()
            if not gdf_points.empty:
                gdf_points.to_file(os.path.join(dest_folder, "colleges_points.gpkg"), driver="GPKG", engine="pyogrio")

            if prog: prog(40, 100)

            # 3. Extraction Couche 2 : Polygones des Communes (WFS IGN API)
            logger.debug("Téléchargement des limites HD (BD TOPO IGN) et jointure...")
            gdf_communes = gpd.GeoDataFrame()
            
            # On prépare la Bounding Box pour l'API de l'IGN (en Lambert 93 / 2154)
            minx, miny, maxx, maxy = bbox_coords
            bbox_str = f"{minx},{miny},{maxx},{maxy},EPSG:2154"
            
            params = {
                'SERVICE': 'WFS',
                'VERSION': '2.0.0',
                'REQUEST': 'GetFeature',
                'TYPENAMES': 'BDTOPO_V3:commune', # La vraie BD TOPO !
                'OUTPUTFORMAT': 'application/json',
                'BBOX': bbox_str,
                'SRSNAME': 'EPSG:2154'
            }
            
            # Appel à l'API de l'IGN
            response = requests.get("https://data.geopf.fr/wfs/ows", params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            features = data.get('features', [])
            
            if features:
                gdf_communes = gpd.GeoDataFrame.from_features(features, crs="EPSG:2154")
                
                # Découpage final avec le polygone exact dessiné par l'utilisateur
                if mask_2154 is not None and not gdf_communes.empty:
                    gdf_communes = gdf_communes[gdf_communes.geometry.intersects(mask_2154)].copy()
                
                if not gdf_communes.empty:
                    # Dans la BD TOPO V3, la colonne s'appelle toujours strictement "code_insee"
                    col_insee = 'code_insee'
                    
                    if col_insee not in gdf_communes.columns:
                        raise KeyError(f"L'API IGN n'a pas renvoyé la colonne '{col_insee}'. Colonnes: {list(gdf_communes.columns)}")

                    # LA JOINTURE avec notre CSV local
                    df_statuts = pd.read_csv(self.path_csv_statuts, sep=';', dtype=str)
                    gdf_communes = gdf_communes.merge(df_statuts, on='code_insee', how='left')
                    
                    gdf_communes.to_file(os.path.join(dest_folder, "secteurs_communes.gpkg"), driver="GPKG", engine="pyogrio")
            else:
                logger.warning("Aucune commune trouvée par l'IGN sur cette zone.")

            if prog: prog(70, 100)

            # 4. Extraction Couche 3 : Le Dictionnaire CSV filtré (Fichier Local)
            if not gdf_communes.empty and os.path.exists(self.path_csv_rues):
                logger.debug("Extraction du Dictionnaire des Rues (CSV)...")
                codes_insee_extraits = gdf_communes['code_insee'].unique()
                
                df_rues = pd.read_csv(self.path_csv_rues, sep=';', dtype=str)
                df_rues_filtre = df_rues[df_rues['code_insee'].isin(codes_insee_extraits)]
                
                if not df_rues_filtre.empty:
                    df_rues_filtre.to_csv(os.path.join(dest_folder, "dictionnaire_rues.csv"), sep=';', index=False, encoding='utf-8-sig')

            if prog: prog(100, 100)
            return True, f"{len(gdf_points)} collèges et les secteurs extraits avec succès (BD TOPO)."

        except Exception as e:
            logger.exception("Erreur lors de la collecte de la Carte Scolaire")
            return False, f"Erreur Carte Scolaire : {str(e)}"