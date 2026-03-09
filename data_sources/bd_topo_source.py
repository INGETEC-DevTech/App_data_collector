# data_sources/bd_topo_source.py

import sys
import os
import datetime
import time 
import requests
import geopandas
import pandas 
import xml.etree.ElementTree as ET 
from shapely.ops import transform
import pyproj

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from .base_source import SourceDeDonneesBase

class BdTopoSource(SourceDeDonneesBase):
    """Source de données pour la BD TOPO via le service WFS de l'IGN."""

    def __init__(self, config: dict):
        super().__init__(config)
        
        wfs_conf = self.config.get("wfs_config", {})
        self.base_url = wfs_conf.get("base_url")
        self.wfs_version = wfs_conf.get("version")
        
        self.layers_config = self.config.get("layers_config", {})
        
        pagination_conf = self.config.get("pagination_config", {})
        self.page_size = pagination_conf.get("default_page_size", 1000) 
        self.max_retries = pagination_conf.get("max_retries", 2)
        self.retry_delay = pagination_conf.get("retry_delay_seconds", 5)

    @property
    def supports_update(self) -> bool:
        """Cette source est "En Ligne", donc pas de mise à jour de fichier local."""
        return False

    def valider_lien(self):
        params = {'SERVICE': 'WFS', 'VERSION': self.wfs_version, 'REQUEST': 'GetCapabilities'}
        try:
            response = requests.get(self.base_url, params=params, timeout=10) 
            response.raise_for_status()
            return True, "Service BD TOPO WFS accessible."
        except requests.exceptions.RequestException as e:
            return False, f"Erreur d'accès à BD TOPO WFS: {e}"

    def get_parametres_specifiques_ui(self):
        return {
            "type": "layer_selection",
            "title": f"Sélection des couches pour {self.nom_source}",
            "layers": self.layers_config
        }

    def formater_options_collecte(self, valeurs_ui: list | None) -> dict:
        """Formate la liste de couches en dictionnaire pour la collecte."""
        return {"selected_typenames": valeurs_ui if isinstance(valeurs_ui, list) else []}
    
    def _appliquer_post_traitement(self, gdf: geopandas.GeoDataFrame, typename: str) -> geopandas.GeoDataFrame:
        """
        Applique les règles de post-traitement (renommage, suppression de colonnes)
        définies dans le fichier de configuration pour une couche donnée.
        """
        layer_conf = self.layers_config.get(typename, {})
        processing_rules = layer_conf.get("post_processing")

        if not processing_rules:
            return gdf # Pas de règles, on retourne le GDF tel quel

        # Renommage des colonnes
        cols_to_rename = processing_rules.get("rename_columns", {})
        if cols_to_rename:
            gdf.rename(columns=cols_to_rename, inplace=True, errors='ignore')

        # Suppression des colonnes
        cols_to_drop = processing_rules.get("drop_columns", [])
        if cols_to_drop:
            # On s'assure que les colonnes à supprimer existent bien dans le dataframe
            existing_cols_to_drop = [col for col in cols_to_drop if col in gdf.columns]
            gdf.drop(columns=existing_cols_to_drop, inplace=True)
            
        return gdf

    def collecter_donnees(self, dossier_export_local, perimetre_selection_objet, options_specifiques):
        log_callback = options_specifiques.get("log_callback", print)
        progress_callback = options_specifiques.get("progress_callback") 
        
        subdirectory_name = self.config.get("export_subdirectory", self.nom_source)
        destination_folder = os.path.join(dossier_export_local, subdirectory_name)
        try:
            os.makedirs(destination_folder, exist_ok=True)
        except OSError as e:
            return False, f"Erreur lors de la création du dossier {destination_folder}: {e}"
        
        selected_typenames = options_specifiques.get("selected_typenames", [])
        if not selected_typenames:
            return False, "Aucune couche BD TOPO sélectionnée."
        
        if not perimetre_selection_objet or perimetre_selection_objet.get("type") != "bbox":
            return False, "Format BBOX invalide."
            
        bbox_value = perimetre_selection_objet["value"]
        bbox_crs = perimetre_selection_objet["crs"]

        # Syntaxe stricte IGN WFS 2.0.0
        epsg_code = bbox_crs.split(':')[1] if ':' in bbox_crs else bbox_crs
        if str(epsg_code) == "4326":
            bbox_str = f"{bbox_value[1]},{bbox_value[0]},{bbox_value[3]},{bbox_value[2]},urn:ogc:def:crs:EPSG::4326"
        else:
            # En WFS 2.0.0, l'IGN exige le format URN complet
            bbox_str = f"{bbox_value[0]},{bbox_value[1]},{bbox_value[2]},{bbox_value[3]},urn:ogc:def:crs:EPSG::{epsg_code}"

        output_srsname = "EPSG:2154"
        output_crs_geopandas = "EPSG:2154"

        # ---------------------------------------------------------------------
        # PHASE 1 : Estimation du volume total
        # ---------------------------------------------------------------------
        log_callback("Estimation du volume des données à télécharger...")
        total_entites_global = 0
        couches_a_traiter = [] 

        for typename in selected_typenames:
            typename_simple = typename.split(':')[-1] if ':' in typename else typename
            params_hits = {'SERVICE': 'WFS', 'VERSION': self.wfs_version, 'REQUEST': 'GetFeature', 'TYPENAMES': typename, 'BBOX': bbox_str, 'RESULTTYPE': 'hits'}
            
            count = -1 # Initialisé à -1 pour repérer les plantages
            
            try:
                hits_response = requests.get(self.base_url, params=params_hits, timeout=10)
                hits_response.raise_for_status()
                root = ET.fromstring(hits_response.content)
                number_matched_str = next((elem.attrib.get('numberMatched') or elem.attrib.get('numberOfFeatures') for elem in root.iter() if 'numberMatched' in elem.attrib or 'numberOfFeatures' in elem.attrib), None)
                if number_matched_str is not None:
                    count = int(number_matched_str)
            except Exception as e:
                log_callback(f"Attention: Comptage échoué pour {typename_simple} ({e}). On lance le téléchargement forcé.")
                if 'hits_response' in locals():
                    log_callback(f"Message caché de l'IGN : {hits_response.text}")
            
            if count > 0:
                total_entites_global += count
                couches_a_traiter.append((typename, count))
            elif count == -1:
                # Si le comptage plante (count reste à -1), on ajoute quand même à la liste 
                # pour que la PHASE 2 nous crache la VRAIE erreur ou télécharge les données.
                total_entites_global += 1000 
                couches_a_traiter.append((typename, 1000))
            else:
                # Ici c'est un vrai 0 (l'IGN a répondu "0 entité")
                log_callback(f"Aucune entité trouvée pour {typename_simple}, ignorée.")

        if total_entites_global == 0 and len(couches_a_traiter) == 0:
             return True, "Aucune donnée trouvée sur l'ensemble des couches sélectionnées."

        log_callback(f"Volume total identifié (estimé) : {total_entites_global} entités.")

        # ---------------------------------------------------------------------
        # PHASE 2 : Téléchargement avec barre de progression globale
        # ---------------------------------------------------------------------
        succes_global = True
        entites_sauvegardees_totales = 0
        progress_counter = 0             # Compteur global pour la barre
        nombre_fichiers_crees = 0
        messages_erreur = []

        # On boucle sur la liste filtrée (celles qui ont des données)
        for typename, count_theorique in couches_a_traiter:
            typename_simple = typename.split(':')[-1] if ':' in typename else typename
            nom_fichier_valide = "".join(c if c.isalnum() else '_' for c in typename_simple)
            chemin_export = os.path.join(destination_folder, f"{nom_fichier_valide}.gpkg")
            
            start_index = 0
            keep_paging = True
            first_chunk = True 
            
            # Gestion du SRSNAME (La toponymie plante souvent si on force EPSG:2154 à la demande)
            if "toponymie" not in typename.lower():
                param_srs = output_srsname
                current_crs_target = output_crs_geopandas
            else:
                param_srs = None 
                current_crs_target = "EPSG:4326"

            while keep_paging:
                params_getfeature = {
                    'SERVICE': 'WFS', 'VERSION': self.wfs_version, 'REQUEST': 'GetFeature', 
                    'TYPENAMES': typename, 'OUTPUTFORMAT': 'application/json', 
                    'BBOX': bbox_str, 'COUNT': self.page_size, 'STARTINDEX': start_index
                }
                if param_srs:
                    params_getfeature['SRSNAME'] = param_srs

                try:
                    wfs_response = requests.get(self.base_url, params=params_getfeature, timeout=180)
                    wfs_response.raise_for_status()
                    features = wfs_response.json().get('features', [])
                    
                    nb_features = len(features)
                    
                    if nb_features > 0:
                        current_page_gdf = geopandas.GeoDataFrame.from_features(features, crs=current_crs_target)
                        
                        if current_page_gdf.crs != output_crs_geopandas:
                            current_page_gdf = current_page_gdf.to_crs(output_crs_geopandas)

                        # --- DÉBUT DU FILTRAGE PAR POLYGONE (CLIPPING) ---
                        mask_2154 = perimetre_selection_objet.get("polygon")
                        if mask_2154 is not None and not current_page_gdf.empty:
                            # Suppression des lignes 'project' et 'transform' devenues inutiles
                            
                            # On garde ce qui touche la commune
                            current_page_gdf = current_page_gdf[current_page_gdf.geometry.intersects(mask_2154)].copy()
                        # --- FIN DU FILTRAGE ---

                        current_page_gdf = self._appliquer_post_traitement(current_page_gdf, typename)
                        
                        write_mode = 'w' if first_chunk else 'a'
                        current_page_gdf.to_file(chemin_export, driver="GPKG", layer=typename_simple, engine="pyogrio", mode=write_mode)
                        
                        entites_sauvegardees_totales += len(current_page_gdf)
                        first_chunk = False 

                    # --- Mise à jour de la barre de progression GLOBALE ---
                    progress_counter += nb_features
                    if progress_callback:
                        # On s'assure de ne pas dépasser 100% si l'API envoie plus que prévu
                        val_progress = min(progress_counter, total_entites_global)
                        progress_callback(val_progress, total_entites_global)
                    
                    if nb_features < self.page_size:
                        keep_paging = False
                    else:
                        start_index += self.page_size

                except Exception as e:
                    messages_erreur.append(f"Erreur couche {typename}: {e}")
                    succes_global = False
                    keep_paging = False 
                    # En cas d'erreur, on "avance" artificiellement la barre pour ne pas qu'elle reste bloquée
                    progress_counter += (count_theorique - start_index)
                    if progress_callback:
                        progress_callback(min(progress_counter, total_entites_global), total_entites_global)
                    time.sleep(self.retry_delay)

            if not first_chunk: 
                nombre_fichiers_crees += 1
    
        if succes_global and entites_sauvegardees_totales > 0:
            summary_message = f"Succès : {entites_sauvegardees_totales} entités BD TOPO récupérées."
        elif entites_sauvegardees_totales == 0 and not messages_erreur:
            summary_message = "Aucun aménagement trouvé dans cette zone."
        else:
            summary_message = f"Erreur BD TOPO : {str(messages_erreur[0]) if messages_erreur else 'Erreur inconnue'}"
            
        return succes_global, summary_message


# --- AJOUT : Fonction utilitaire pour récupérer la géométrie précise ---
def recuperer_geometrie_precise_ign(type_territoire: str, code_territoire: str, log_callback=print):
    """
    Récupère la géométrie précise (Polygone) d'une commune ou d'un EPCI
    directement depuis le WFS de l'IGN (BD TOPO).
    Retourne un objet Shapely Geometry ou None en cas d'échec.
    """
    if not code_territoire:
        return None

    # URL stable du WFS IGN
    wfs_url = "https://data.geopf.fr/wfs/ows"
    
    # Configuration des filtres selon le type
    if type_territoire == "Commune":
        typename = "BDTOPO_V3:commune"
        # Le champ standard est 'code_insee' pour les communes
        cql_filter = f"code_insee='{code_territoire}'"
    elif type_territoire == "EPCI":
        typename = "BDTOPO_V3:epci"
        # Le champ standard est 'code_siren' pour les EPCI
        cql_filter = f"code_siren='{code_territoire}'"
    else:
        return None

    params = {
        'SERVICE': 'WFS',
        'VERSION': '2.0.0',
        'REQUEST': 'GetFeature',
        'TYPENAMES': typename,
        'OUTPUTFORMAT': 'application/json',
        'CQL_FILTER': cql_filter,
        'SRSNAME': 'EPSG:2154' # On demande explicitement du Lambert 93
    }

    try:
        response = requests.get(wfs_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        features = data.get('features', [])
        
        if features:
            # On utilise GeoPandas pour convertir proprement le GeoJSON en objet géométrique
            gdf = geopandas.GeoDataFrame.from_features(features, crs="EPSG:2154")
            if not gdf.empty:
                poly = gdf.geometry.iloc[0]
                return poly
            
    except Exception as e:
        log_callback(f"Erreur WFS IGN (Géométrie précise) : {e}")
    
    return None