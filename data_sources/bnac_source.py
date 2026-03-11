# data_sources/bnac_source.py

import os
import geopandas as gpd
from data_sources.base_source import SourceDeDonneesBase
from pyproj import Transformer # Import indispensable pour la transformation de coordonnées
from shapely.ops import transform
import pyproj

class BnacSource(SourceDeDonneesBase):
    def __init__(self, config: dict):
        super().__init__(config)
        # Récupération de la configuration locale
        file_conf = self.config.get("local_file_config", {})
        self.filepath = file_conf.get("path")
        self.native_crs = file_conf.get("native_crs", "EPSG:4326")

    @property
    def supports_update(self) -> bool:
        """Le fichier local est statique."""
        return True

    def valider_lien(self):
        """Vérifie si le fichier GeoJSON existe sur le disque."""
        if self.filepath and os.path.exists(self.filepath):
            return True, f"Fichier BNAC trouvé : {os.path.basename(self.filepath)}"
        return False, f"Fichier BNAC introuvable : {self.filepath}"

    def get_parametres_specifiques_ui(self):
        return None

    def formater_options_collecte(self, valeurs_ui) -> dict:
        return {}

    def collecter_donnees(self, dossier_export_local, perimetre_selection_objet, options_specifiques):
        log = options_specifiques.get("log_callback", print)
        prog = options_specifiques.get("progress_callback")

        try:
            log("Démarrage de la collecte : Aménagements cyclables (BNAC)")
            
            # 1. Préparation du filtre spatial
            bbox_coords = perimetre_selection_objet["value"] 
            source_crs = perimetre_selection_objet.get("crs", "EPSG:2154")

            log("  > [Action] : Préparation du filtre spatial...")
            # On transforme la BBOX vers le CRS natif du fichier (4326)
            transformer = Transformer.from_crs(source_crs, self.native_crs, always_xy=True)
            minx, miny = transformer.transform(bbox_coords[0], bbox_coords[1])
            maxx, maxy = transformer.transform(bbox_coords[2], bbox_coords[3])
            filter_bbox = (minx, miny, maxx, maxy)

            # 2. Lecture locale filtrée
            log(f"  > [Action] : Lecture du fichier local {os.path.basename(self.filepath)}...")
            if prog: prog(20, 100)
            
            # Lecture optimisée avec pyogrio et filtre spatial
            gdf = gpd.read_file(self.filepath, bbox=filter_bbox, engine="pyogrio")

            if gdf.empty:
                return True, f"[{self.nom_source}] : Aucun aménagement cyclable trouvé dans cette zone."

            # 3. Projection et finalisation
            log(f"  > [Filtrage] : {len(gdf)} segments cyclables identifiés.")
            if prog: prog(80, 100)
            
            gdf = gdf.to_crs("EPSG:2154")
            
            dest_folder = os.path.join(dossier_export_local, self.config.get("export_subdirectory", "VELO"))
            os.makedirs(dest_folder, exist_ok=True)
            
            path_out = os.path.join(dest_folder, "aménagements_cyclables_bnac.gpkg")
            log(f"  > [Action] : Sauvegarde vers {os.path.basename(path_out)}...")

            # --- FILTRAGE PAR POLYGONE ---
            mask_2154 = perimetre_selection_objet.get("polygon")
            if mask_2154 is not None and not gdf.empty:
                # On utilise directement le masque déjà en 2154
                gdf = gdf[gdf.geometry.within(mask_2154)].copy()
            
            gdf.to_file(path_out, driver="GPKG", engine="pyogrio")

            if prog: prog(100, 100)
            return True, f"Succès : {len(gdf)} aménagements cyclables récupérés."

        except Exception as e:
            log(f"  ERREUR BNAC : {str(e)}")
            return False, f"Erreur BNAC (Local) : {str(e)}"