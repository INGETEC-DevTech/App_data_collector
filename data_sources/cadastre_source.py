# data_sources/cadastre_source.py

import os, time, requests, geopandas as gpd, xml.etree.ElementTree as ET

from .base_source import SourceDeDonneesBase
from logger_config import logger

try:
    from .enrichment_pm import enrich_parcels_with_pm_data
    ENRICHMENT_PM_AVAILABLE = True
except ImportError:
    ENRICHMENT_PM_AVAILABLE = False
    logger.warning("Fichier enrichment_pm.py non trouvé. L'enrichissement PM sera désactivé.")

class CadastreSource(SourceDeDonneesBase):
    def __init__(self, config: dict):
        super().__init__(config)
        wfs_conf = self.config.get("wfs_config", {})
        self.base_url = wfs_conf.get("base_url")
        self.wfs_version = wfs_conf.get("version")
        self.typename_parcelles = wfs_conf.get("typename_parcelles")

        pag_conf = self.config.get("pagination_config", {})
        self.page_size = pag_conf.get("default_page_size", 1000)
        self.max_retries = pag_conf.get("max_retries", 2)
        self.retry_delay = pag_conf.get("retry_delay_seconds", 5)
        
        self.enrich_conf = self.config.get("enrichment_pm_config", {})
        self.pm_enrichment_possible = (
            ENRICHMENT_PM_AVAILABLE and 
            self.enrich_conf.get("enabled", False) and 
            os.path.isdir(self.enrich_conf.get("csv_directory_path", ""))
        )
        if not self.typename_parcelles:
            logger.error(f"Erreur de configuration : 'typename_parcelles' non configuré pour {self.nom_source}.")

    @property
    def supports_update(self) -> bool:
        """Source 'En Ligne', pas de mise à jour de fichier local."""
        return False

    def valider_lien(self):
        try:
            params = {'SERVICE': 'WFS','VERSION': self.wfs_version,'REQUEST': 'GetCapabilities'}
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            wfs_msg = "Service WFS accessible."
            wfs_ok = True
        except requests.RequestException:
            wfs_msg = "Service WFS inaccessible."
            wfs_ok = False
        
        pm_msg = "Enrichissement PM désactivé."
        if self.pm_enrichment_possible:
            pm_msg = "Enrichissement PM disponible."
            
        return wfs_ok, f"WFS: {wfs_msg} | {pm_msg}"

    def get_parametres_specifiques_ui(self):
        if not self.pm_enrichment_possible:
            return None
        return {
            "type": "checkbox_options", 
            "title": f"Options pour {self.nom_source}",
            "options": [{
                "id": "enrichir_pm", 
                "label": "Enrichir avec les données Personnes Morales",
                "default_checked": True 
            }]
        }

    def formater_options_collecte(self, valeurs_ui) -> dict:
        return {"options": valeurs_ui if isinstance(valeurs_ui, list) else []}

    def collecter_donnees(self, dossier_export_local, perimetre_selection_objet, options_specifiques):
        progress_callback = options_specifiques.get("progress_callback")
        t_debut_collecte = time.perf_counter()

        subdirectory_name = self.config.get("export_subdirectory", self.nom_source)
        destination_folder = os.path.join(dossier_export_local, subdirectory_name)
        try:
            os.makedirs(destination_folder, exist_ok=True)
        except OSError as e:
            return False, f"Erreur lors de la création du dossier {destination_folder}: {e}"

        enrichir_pm = any(opt.get("checked") for opt in options_specifiques.get("options", []) if opt.get("id") == "enrichir_pm")
        if enrichir_pm and not self.pm_enrichment_possible:
            logger.warning("Enrichissement PM demandé mais non possible.")
            enrichir_pm = False

        if not self.typename_parcelles: return False, "TYPENAME des parcelles non configuré."
        if not perimetre_selection_objet or "value" not in perimetre_selection_objet: return False, "Périmètre ou coordonnées introuvables."
        
        bbox, crs = perimetre_selection_objet["value"], perimetre_selection_objet["crs"]
        bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]},urn:ogc:def:crs:EPSG::{crs.split(':')[1]}"
        out_crs = "EPSG:2154"
        
        nom_fichier = "cadastre_parcelles.gpkg"
        chemin_export = os.path.join(destination_folder, nom_fichier)
        start_index = 0
        total_parcelles = 0
        first_chunk = True 
        
        total_theorique = 0
        params_hits = {'SERVICE': 'WFS', 'VERSION': self.wfs_version, 'REQUEST': 'GetFeature', 'TYPENAMES': self.typename_parcelles, 'BBOX': bbox_str, 'RESULTTYPE': 'hits'}
        try:
            hits_res = requests.get(self.base_url, params=params_hits, timeout=30)
            if hits_res.status_code == 200:
                root = ET.fromstring(hits_res.content)
                number_matched_str = next((elem.attrib.get('numberMatched') or elem.attrib.get('numberOfFeatures') for elem in root.iter() if 'numberMatched' in elem.attrib or 'numberOfFeatures' in elem.attrib), None)
                if number_matched_str:
                    total_theorique = int(number_matched_str)
        except: pass
        
        while True:
            params = {
                'SERVICE':'WFS','VERSION':self.wfs_version,'REQUEST':'GetFeature',
                'TYPENAMES':self.typename_parcelles,'OUTPUTFORMAT':'application/json',
                'BBOX':bbox_str,'SRSNAME':f"urn:ogc:def:crs:EPSG::{out_crs.split(':')[1]}",
                'COUNT':self.page_size,'STARTINDEX':start_index
            }

            # -Logique de reessai en cas de crash
            features = None
            max_retries = 3
            retry_count = 0

            while retry_count < max_retries:
                try:
                    logger.debug(f"Récupération index {start_index} ({self.page_size} parcelles)...")
                    response = requests.get(self.base_url, params=params, timeout=180)
                    response.raise_for_status()
                    features = response.json().get('features', [])
                    break  # Si on arrive ici, la requête a réussi, on sort de la boucle de retry
                
                except Exception as e:
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"Tentative {retry_count} échouée ({e}). Nouvel essai dans 3s...")
                        time.sleep(3) # On attend un peu avant de réessayer
                    else:
                        # Si on a épuisé les tentatives
                        logger.error(f"Erreur WFS persistante à l'index {start_index}: {e}")
                        return False, f"Échec de la collecte WFS après {max_retries} tentatives: {e}"

            if not features:
                break

            chunk_gdf = gpd.GeoDataFrame.from_features(features, crs=out_crs)

            # --- DÉBUT DU FILTRAGE PAR POLYGONE (CLIPPING) ---
            mask_2154 = perimetre_selection_objet.get("polygon")
            
            if mask_2154 is not None and not chunk_gdf.empty:    
                # On garde les parcelles qui intersectent la commune
                chunk_gdf = chunk_gdf[chunk_gdf.geometry.intersects(mask_2154)].copy()
            # --- FIN DU FILTRAGE ---

            if not chunk_gdf.empty: # On ne sauvegarde que si le filtrage a laissé des données
                write_mode = 'w' if first_chunk else 'a'
                chunk_gdf.to_file(chemin_export, driver="GPKG", layer="parcelles", engine="pyogrio", mode=write_mode)
                total_parcelles += len(chunk_gdf)
                first_chunk = False

            if progress_callback and total_theorique > 0:
                # Note: ici il vaut mieux utiliser start_index pour la barre 
                # car total_parcelles diminue à cause du filtrage polygone
                progress_callback(min(start_index + len(features), total_theorique), total_theorique)

            if len(features) < self.page_size:
                break
            start_index += len(features)
        
        if total_parcelles == 0:
            return True, f"[{self.nom_source}] : Aucune parcelle trouvée."

        message_final = f"{total_parcelles} parcelles sauvegardées avec succès."
        
        if enrichir_pm:
            logger.info("Lancement de l'enrichissement Personnes Morales...") 
            enrich_success = enrich_parcels_with_pm_data(chemin_export, "parcelles", self.enrich_conf, logger.debug)
            if enrich_success:
                message_final = f"{total_parcelles} parcelles sauvegardées et enrichies avec succès."

        # Log final en interne (on garde la durée ici pour le log technique interne)
        logger.debug(f"Collecte {self.nom_source} terminée en {time.perf_counter() - t_debut_collecte:.2f}s.")
        
        return True, f"{message_final}"