import os
import requests
import geopandas as gpd
import pandas as pd
from data_sources.base_source import SourceDeDonneesBase
from logger_config import logger

class BpeSource(SourceDeDonneesBase):
    def __init__(self, config: dict):
        super().__init__(config)
        self.filepath = self.config.get("local_file_config", {}).get("path") 
        self.filepath_scores = self.config.get("local_file_config_scores", {}).get("path") 

    @property
    def supports_update(self) -> bool:
        return True

    def valider_lien(self):
        if self.filepath and os.path.exists(self.filepath) and self.filepath_scores and os.path.exists(self.filepath_scores):
            return True, f"Base BPE (Points et Scores) trouvée."
        return False, "Fichier(s) local/locaux introuvable(s)."

    def get_parametres_specifiques_ui(self):
        return {
            "type": "checkbox_options",
            "title": "Options d'export de la BPE",
            "options": [
                {"id": "export_points", "label": "Localisation des équipements (Points)", "default_checked": True},
                {"id": "export_communes", "label": "Classification des communes (Polygones)", "default_checked": True}
            ]
        }

    def formater_options_collecte(self, valeurs_ui) -> dict:
        options = {"export_points": True, "export_communes": True}
        if valeurs_ui:
            for item in valeurs_ui: options[item["id"]] = item["checked"]
        return options

    def collecter_donnees(self, dossier_export_local, perimetre_selection_objet, options_specifiques):
        prog = options_specifiques.get("progress_callback")

        try:         
            # 1. Récupération des Communes IGN
            logger.debug("Téléchargement des contours de communes (IGN WFS)...")
            bbox_coords = perimetre_selection_objet["value"] 
            bbox_str = f"{bbox_coords[0]},{bbox_coords[1]},{bbox_coords[2]},{bbox_coords[3]},urn:ogc:def:crs:EPSG::2154"
            
            url_ign = "https://data.geopf.fr/wfs/ows"
            res = requests.get(url_ign, params={
                'SERVICE': 'WFS', 'VERSION': '2.0.0', 'REQUEST': 'GetFeature',
                'TYPENAMES': 'BDTOPO_V3:commune', 'BBOX': bbox_str,
                'OUTPUTFORMAT': 'application/json', 'SRSNAME': 'EPSG:2154'
            }, timeout=30)
            res.raise_for_status()
            
            gdf_communes = gpd.GeoDataFrame.from_features(res.json(), crs="EPSG:2154")
            
            polygon_mask = perimetre_selection_objet.get("polygon")
            if polygon_mask is not None:
                gdf_communes = gdf_communes[gdf_communes.geometry.centroid.intersects(polygon_mask)].copy()
            if prog: prog(40, 100)

            # --- DÉBUT DES EXPORTS ---
            dest_folder = os.path.join(dossier_export_local, self.config.get("export_subdirectory", "EQUIPEMENTS"))
            os.makedirs(dest_folder, exist_ok=True)

            # =====================================================================
            # EXPORT DES COMMUNES (POLYGONES)
            # =====================================================================
            if options_specifiques.get("export_communes", True):
                logger.debug("Génération de la couche Communes...")
                df_scores = pd.read_csv(self.filepath_scores, sep=';', dtype={'code_insee': str})
                gdf_poles = gdf_communes.merge(df_scores, on='code_insee', how='left')
                
                # Remplissage des communes sans équipements
                gdf_poles['Echelon'] = gdf_poles['Echelon'].fillna("Commune non-pôle")
                for col in ['Score proximité', 'Score intermédiaire', 'Score supérieur']:
                    gdf_poles[col] = gdf_poles[col].fillna(0.0)
                
                # RENOMMAGE ET FILTRAGE DES COLONNES
                gdf_poles = gdf_poles.rename(columns={'nom_officiel': 'Commune'})
                colonnes_a_garder = ['code_insee', 'Commune', 'Score proximité', 'Score intermédiaire', 'Score supérieur', 'Echelon', 'geometry']
                gdf_poles = gdf_poles[colonnes_a_garder]
                
                # EXPORT (Nom du fichier identique au nom de la couche)
                path_poles = os.path.join(dest_folder, "classification_commune.gpkg")
                gdf_poles.to_file(path_poles, driver="GPKG", layer="classification_commune")
                logger.debug("Fichier 'classification_commune.gpkg' généré (colonnes épurées).")

            if prog: prog(70, 100)

            # =====================================================================
            # EXPORT DES POINTS (ÉQUIPEMENTS)
            # =====================================================================
            nb_equipements = 0
            if options_specifiques.get("export_points", True):
                logger.debug("Génération de la couche Équipements...")
                filter_bbox = (bbox_coords[0], bbox_coords[1], bbox_coords[2], bbox_coords[3])
                gdf_bpe_points = gpd.read_file(self.filepath, bbox=filter_bbox, engine="pyogrio")
                
                if polygon_mask is not None:
                    gdf_bpe_points = gdf_bpe_points[gdf_bpe_points.geometry.intersects(polygon_mask)].copy()

                if not gdf_bpe_points.empty:
                    # 1. Jointure pour récupérer le nom de la commune (nom_officiel) via le code INSEE (DEPCOM)
                    gdf_bpe_points = gdf_bpe_points.merge(
                        gdf_communes[['code_insee', 'nom_officiel']], 
                        left_on='DEPCOM', 
                        right_on='code_insee', 
                        how='left'
                    )
                    
                    # 2. RENOMMAGE DES COLONNES
                    dictionnaire_renommage = {
                        'NOMRS': 'NOM',
                        'TYPEQU': 'CODE',
                        'Libelle_TYPEQU': 'TYPE',
                        'nom_officiel': 'COMMUNE',
                        'Libelle_SDOM': 'SOUS-DOMAINE',
                        'Libelle_DOM': 'DOMAINE',
                        'gamme': 'GAMME'
                    }
                    gdf_bpe_points = gdf_bpe_points.rename(columns=dictionnaire_renommage)
                    
                    # 3. FILTRAGE ET MISE DANS L'ORDRE
                    colonnes_equipements = ['NOM', 'CODE', 'TYPE', 'COMMUNE', 'SOUS-DOMAINE', 'DOMAINE', 'GAMME', 'geometry']
                    
                    # On s'assure de ne garder que les colonnes qui existent bien
                    colonnes_finales = [col for col in colonnes_equipements if col in gdf_bpe_points.columns]
                    gdf_bpe_points = gdf_bpe_points[colonnes_finales]
                    
                    # EXPORT (Nom du fichier identique au nom de la couche)
                    path_points = os.path.join(dest_folder, "equipements_bpe.gpkg")
                    gdf_bpe_points.to_file(path_points, driver="GPKG", layer="equipements_bpe")
                    logger.debug("Fichier 'equipements_bpe.gpkg' généré (colonnes épurées et triées).")
                    nb_equipements = len(gdf_bpe_points)
                else:
                    logger.info("Aucun équipement trouvé dans cette zone.")

            if prog: prog(100, 100)
            return True, f"{nb_equipements} équipements (et classification) sauvegardés avec succès."

        except Exception as e:
            logger.exception(f"Erreur critique lors de la collecte BPE")
            return False, f"Erreur BPE : {str(e)}"
        