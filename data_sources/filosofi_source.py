# data_sources/filosofi_source.py

import os
import geopandas as gpd
from shapely.geometry import box
import time

import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from .base_source import SourceDeDonneesBase
from logger_config import logger

class FilosofiSource(SourceDeDonneesBase):
    def __init__(self, config: dict):
        super().__init__(config)
        file_conf = self.config.get("local_file_config", {})
        self.filepath = file_conf.get("path")
        self.native_crs = file_conf.get("native_crs")
        self.layer_name = file_conf.get("layer_name")

    @property
    def supports_update(self) -> bool:
        """Cette source est un Fichier Local, donc la mise à jour est supportée."""
        return True

    def valider_lien(self):
        if not self.filepath:
            return False, "Chemin du fichier Filosofi non configuré."
        if os.path.exists(self.filepath):
            return True, f"Fichier Filosofi trouvé : {self.filepath}"
        return False, "Fichier local introuvable."

    def get_parametres_specifiques_ui(self):
        """Pas d'options de configuration spécifiques pour cette source."""
        return None

    def formater_options_collecte(self, valeurs_ui) -> dict:
        """Pas d'options, retourne un dictionnaire vide."""
        return {}

    def collecter_donnees(self, dossier_export_local, perimetre_selection_objet, options_specifiques):
        # Récupération du callback de progression
        progress_callback = options_specifiques.get("progress_callback")
        t_debut_collecte = time.perf_counter()

        is_valid, message = self.valider_lien()
        if not is_valid:
            logger.error(message)
            return False, message
        
        # On vérifie juste qu'on a bien reçu un périmètre et ses coordonnées (value)
        if not perimetre_selection_objet or "value" not in perimetre_selection_objet:
            message = "Périmètre ou coordonnées introuvables."
            logger.error(message)
            return False, message

        try:
            # Initialisation de la barre à 0%
            if progress_callback:
                progress_callback(0, 100)

            subdirectory_name = self.config.get("export_subdirectory", self.nom_source)
            destination_folder = os.path.join(dossier_export_local, subdirectory_name)
            os.makedirs(destination_folder, exist_ok=True)

            bbox_coords = perimetre_selection_objet["value"]
            bbox_crs_from_ui = perimetre_selection_objet["crs"]

            selection_polygon_ui_crs = box(*bbox_coords)
            selection_gdf_ui_crs = gpd.GeoDataFrame([{'id': 1, 'geometry': selection_polygon_ui_crs}], crs=bbox_crs_from_ui)

            if bbox_crs_from_ui.upper() != self.native_crs.upper():
                logger.debug(f"Reprojection de la BBOX de sélection vers {self.native_crs}...")
                selection_gdf_native_crs = selection_gdf_ui_crs.to_crs(self.native_crs)
            else:
                selection_gdf_native_crs = selection_gdf_ui_crs
            
            bbox_for_readfile = tuple(selection_gdf_native_crs.total_bounds)
            
            # Auto-détection du nom de la couche
            import pyogrio
            layers = pyogrio.list_layers(self.filepath)
            if len(layers) == 0:
                return False, "Le fichier GeoPackage semble vide ou corrompu."
            
            actual_layer_name = layers[0][0] # On prend la première couche trouvée
            
            logger.debug(f"Lecture filtrée du GeoPackage : {self.filepath}...")
            logger.debug(f"Couche détectée automatiquement : '{actual_layer_name}'")
            
            # Utilisation de pyogrio avec le VRAI nom de la couche
            gdf_filtre = gpd.read_file(self.filepath, layer=actual_layer_name, bbox=bbox_for_readfile, engine="pyogrio")
            
            # --- BLOC DE DÉCOUPAGE PRÉCIS (CLIPPING) ---
            mask_native = perimetre_selection_objet.get("polygon")
            if mask_native is not None and not gdf_filtre.empty:
                logger.debug("Filtrage spatial précis des carreaux FiLoSoFi...")
                # On ne garde que les carreaux qui intersectent la commune/EPCI
                gdf_filtre = gdf_filtre[gdf_filtre.geometry.intersects(mask_native)].copy()

            logger.debug(f"{len(gdf_filtre)} carreaux trouvés. Préparation de la sauvegarde...")
            if progress_callback:
                progress_callback(50, 100) # On montre qu'on a fait la moitié du chemin

            nb_entites_filtrees = len(gdf_filtre)
            logger.debug(f"{nb_entites_filtrees} carreaux FilosoFI trouvés.")

            if nb_entites_filtrees > 0:
                output_crs = "EPSG:2154"
                if gdf_filtre.crs.to_string().upper() != output_crs.upper():
                    logger.debug(f"Reprojection vers {output_crs}...")
                    gdf_filtre = gdf_filtre.to_crs(output_crs)
                
                output_layer_name = "filosofi"
                nom_fichier = f"{output_layer_name}.gpkg"
                chemin_export_complet = os.path.join(destination_folder, nom_fichier)
                
                logger.debug(f"Sauvegarde de {nb_entites_filtrees} carreaux vers {chemin_export_complet}...")
                # ÉCRITURE OPTIMISÉE AVEC PYOGRIO
                gdf_filtre.to_file(chemin_export_complet, driver="GPKG", layer=output_layer_name, engine="pyogrio")
                
                # Mise à jour de la barre à 100%
                if progress_callback:
                    progress_callback(100, 100)
                
                # Libération immédiate de la mémoire
                del gdf_filtre
                
                message_final = f"{nb_entites_filtrees} carreaux sauvegardés avec succès."
                logger.debug(f"Collecte {self.nom_source} terminée en {time.perf_counter() - t_debut_collecte:.2f} sec.")
                return True, f"{message_final}"
            else:
                if progress_callback:
                    progress_callback(100, 100)
                message_final = "Aucun carreau FilosoFI trouvé dans la sélection."
                logger.info(message_final)
                return True, f"{message_final}"

        except Exception as e:
            logger.exception(f"Erreur lors du traitement de FilosoFI")
            return False, f"Erreur lors du traitement de FilosoFI : {e}"