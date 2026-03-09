# data_sources/sirene_source.py

import sys, os, time, requests, pandas as pd, geopandas as gpd
from shapely.ops import transform
import pyproj

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from .base_source import SourceDeDonneesBase

class SireneSource(SourceDeDonneesBase):
    def __init__(self, config: dict):
        super().__init__(config)
        api_conf = self.config.get("api_config", {})
        self.base_url = api_conf.get("base_url")
        self.api_key = api_conf.get("api_key")
        self.api_header_name = api_conf.get("api_header_name")
        self.rate_limit_delay = 60 / api_conf.get("requests_per_minute_limit", 30)
        self.last_request_time = 0

    @property
    def supports_update(self) -> bool:
        """Source 'En Ligne', pas de mise à jour de fichier local."""
        return False

    def get_parametres_specifiques_ui(self):
        return None

    def formater_options_collecte(self, valeurs_ui) -> dict:
        return {}

    def _make_api_request(self, endpoint_url, params=None, log_callback=print):
        # On tente jusqu'à 3 fois en cas de blocage
        for attempt in range(3):
            # 1. Gestion du délai "normal" (lissage des requêtes)
            time_since_last = time.time() - self.last_request_time
            if time_since_last < self.rate_limit_delay:
                time.sleep(self.rate_limit_delay - time_since_last)
            
            headers = {self.api_header_name: self.api_key, "Accept": "application/json"}
            self.last_request_time = time.time()
            
            try:
                response = requests.get(endpoint_url, headers=headers, params=params, timeout=30)
                
                # 2. Si on est bloqué par l'API (Erreur 429)
                if response.status_code == 429:
                    log_callback(f"Quota API atteint (429). Pause de 65 secondes avant reprise...")
                    time.sleep(65) # On attend 1 minute et 5 secondes par sécurité
                    continue # On recommence la boucle (nouvelle tentative)
                
                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError as http_err:
                # Sécurité supplémentaire : Si c'est une 429 qui a échappé au 'if' ci-dessus
                if response.status_code == 429:
                     log_callback(f"Quota API atteint (429). Pause de 65 secondes...")
                     time.sleep(65)
                     continue
                
                log_callback(f"  Erreur HTTP API SIRENE: {http_err} - Réponse: {http_err.response.text}")
                return None # Erreur fatale autre que 429 (ex: 404, 500)
                
            except requests.exceptions.RequestException as req_err:
                log_callback(f"  Erreur Requête API SIRENE: {req_err}")
                return None

        log_callback("Abandon après 3 tentatives échouées (Erreur 429 persistante).")
        return None

    def valider_lien(self):
        endpoint_info = f"{self.base_url}/informations"
        data = self._make_api_request(endpoint_info)
        if data and data.get("etatService") == "UP":
            return True, "API SIRENE accessible."
        status = data.get("etatService", "Inconnu") if data else "Échec"
        return False, f"API SIRENE inaccessible (Statut: {status})."
    
    def collecter_donnees(self, dossier_export_local, perimetre_selection_objet, options_specifiques):
        log_callback = options_specifiques.get("log_callback", print)
        # 1. Récupération du callback de progression
        progress_callback = options_specifiques.get("progress_callback")
        log_callback(f"Début collecte {self.nom_source}...")

        subdirectory_name = self.config.get("export_subdirectory", self.nom_source)
        destination_folder = os.path.join(dossier_export_local, subdirectory_name)
        try:
            os.makedirs(destination_folder, exist_ok=True)
        except OSError as e:
            return False, f"Erreur lors de la création du dossier {destination_folder}: {e}"

        if not (perimetre_selection_objet and perimetre_selection_objet.get("type") == "bbox"):
            return False, "Périmètre BBOX requis."
        
        min_x, min_y, max_x, max_y = perimetre_selection_objet["value"]
        query = f"coordonneeLambertAbscisseEtablissement:[{min_x} TO {max_x}] AND coordonneeLambertOrdonneeEtablissement:[{min_y} TO {max_y}]"
        fields = "siren,nic,siret,dateCreationEtablissement,trancheEffectifsEtablissement,anneeEffectifsEtablissement,denominationUniteLegale,activitePrincipaleUniteLegale,categorieEntreprise,categorieJuridiqueUniteLegale,codePostalEtablissement,libelleCommuneEtablissement,coordonneeLambertAbscisseEtablissement,coordonneeLambertOrdonneeEtablissement,etatAdministratifUniteLegale"
        
        nom_fichier = "sirene_etablissements"
        chemin_gpkg = os.path.join(destination_folder, f"{nom_fichier}.gpkg")
        
        cursor, page = "*", 1
        total_final_sauvegarde = 0
        total_theorique = 0 # 2. Variable pour le maximum de la barre
        first_chunk = True

        # Dictionnaires de mapping (identiques à votre version)
        effectifs_map_tranche = {
            '00': '0 salarié', '01': '1 ou 2 salariés', '02': '3 à 5 salariés',
            '03': '6 à 9 salariés', '11': '10 à 19 salariés', '12': '20 à 49 salariés',
            '21': '50 à 99 salariés', '22': '100 à 199 salariés', '31': '200 à 249 salariés',
            '32': '250 à 499 salariés', '41': '500 à 999 salariés', '42': '1 000 à 1 999 salariés',
            '51': '2 000 à 4 999 salariés', '52': '5 000 à 9 999 salariés', '53': '10 000 salariés et plus'
        }
        eff_map_nb = {'00':0,'01':2,'02':5,'03':10,'11':20,'12':50,'21':100,'22':200,'31':250,'32':500,'41':1000,'42':2000,'51':5000,'52':10000,'53':10000}

        while True:
            params = {"q": query, "champs": fields, "nombre": 1000, "tri": "siret asc", "curseur": cursor}
            endpoint_url = f"{self.base_url}/siret"
            response_data = self._make_api_request(endpoint_url, params=params, log_callback=log_callback)
            
            if not response_data or response_data.get("header", {}).get("statut") != 200:
                msg = response_data.get("fault", {}).get("message", "Erreur API") if response_data else "Échec requête."
                return False, f"Erreur API SIRENE: {msg}"

            # 3. Récupération du total théorique lors de la première page
            if page == 1:
                total_theorique = response_data.get("header", {}).get("total", 0)

            etablissements_page = response_data.get("etablissements", [])
            if not etablissements_page: break

            # --- TRAITEMENT DU CHUNK (Morceau) ---
            df_chunk = pd.json_normalize(etablissements_page)
            
            # Renommage
            rename_map = {
                "uniteLegale.etatAdministratifUniteLegale": "etatAdministratifUniteLegale",
                "uniteLegale.denominationUniteLegale": "denominationUniteLegale",
                "uniteLegale.activitePrincipaleUniteLegale": "activitePrincipaleUniteLegale",
                "uniteLegale.categorieEntreprise": "categorieEntreprise",
                "uniteLegale.categorieJuridiqueUniteLegale": "categorieJuridiqueUniteLegale",
                "adresseEtablissement.codePostalEtablissement": "codePostalEtablissement",
                "adresseEtablissement.libelleCommuneEtablissement": "libelleCommuneEtablissement",
                "adresseEtablissement.coordonneeLambertAbscisseEtablissement": "coordonneeLambertAbscisseEtablissement",
                "adresseEtablissement.coordonneeLambertOrdonneeEtablissement": "coordonneeLambertOrdonneeEtablissement",
                "trancheEffectifsEtablissement": "codeTranche"
            }
            df_chunk.rename(columns=rename_map, inplace=True)
            
            # Filtrages métier
            df_chunk = df_chunk[df_chunk.get('etatAdministratifUniteLegale') == 'A'].copy()
            df_chunk.dropna(subset=['denominationUniteLegale'], inplace=True)
            df_chunk['categorieJuridiqueUniteLegale'] = pd.to_numeric(df_chunk['categorieJuridiqueUniteLegale'], errors='coerce')
            df_chunk = df_chunk[df_chunk['categorieJuridiqueUniteLegale'] > 2000].copy()

            # Mapping effectifs
            if 'codeTranche' in df_chunk.columns:
                df_chunk['nombreEmploye'] = df_chunk['codeTranche'].map(eff_map_nb)
                df_chunk['trancheNombreEmploye'] = df_chunk['codeTranche'].map(effectifs_map_tranche)
                df_chunk = df_chunk[df_chunk['nombreEmploye'].fillna(0) > 0].copy()

            # --- ÉCRITURE DU CHUNK SI NON VIDE ---
            if not df_chunk.empty:
                col_x, col_y = 'coordonneeLambertAbscisseEtablissement', 'coordonneeLambertOrdonneeEtablissement'
                df_geo = df_chunk.dropna(subset=[col_x, col_y]).copy()
                # ... (conversion et création gdf_chunk) ...
                
                if not df_geo.empty:
                    gdf_chunk = gpd.GeoDataFrame(df_geo, geometry=gpd.points_from_xy(df_geo[col_x], df_geo[col_y]), crs="EPSG:2154")

                    # --- DÉBUT DU BLOC DE FILTRAGE PAR POLYGONE ---
                    mask_2154 = perimetre_selection_objet.get("polygon")
                    if mask_2154 is not None and not gdf_chunk.empty:
                        # On ne garde que les points strictement à l'intérieur
                        gdf_chunk = gdf_chunk[gdf_chunk.geometry.within(mask_2154)].copy()
                    # --- FIN DU BLOC DE FILTRAGE ---

                    write_mode = 'w' if first_chunk else 'a'
                    gdf_chunk.to_file(chemin_gpkg, driver="GPKG", engine="pyogrio", mode=write_mode)
                    
                    # 4. Mise à jour de la progression
                    # Note : on utilise ici le compteur total d'établissements reçus (page * 1000)
                    # car le filtrage réduit le nombre final, ce qui ferait "reculer" la barre.
                    total_recu_estime = page * 1000
                    if progress_callback and total_theorique > 0:
                        # On s'assure de ne pas dépasser le total théorique
                        progress_callback(min(total_recu_estime, total_theorique), total_theorique)
                    
                    total_final_sauvegarde += len(gdf_chunk)
                    first_chunk = False

            log_callback(f"  Page {page} traitée.")
            page += 1
            if not (cursor := response_data.get("header", {}).get("curseurSuivant")): break
        
        return True, f"[{self.nom_source}] : {total_final_sauvegarde} établissements sauvegardés."