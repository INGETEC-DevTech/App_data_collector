import unicodedata
from PyQt6.QtCore import QStringListModel
from PyQt6.QtWidgets import QCompleter

import requests
import geopandas
from logger_config import logger

def nettoyer_texte(texte):
    if not texte: return ""
    texte = str(texte).lower()
    texte = texte.replace('œ', 'oe').replace('æ', 'ae').replace('ç', 'c')
    texte = texte.replace('-', '').replace("'", '').replace(' ', '')
    texte = "".join(c for c in unicodedata.normalize('NFD', texte) if unicodedata.category(c) != 'Mn')
    return " ".join(texte.split())

class CompleterIntelligent(QCompleter):
    def __init__(self, items, parent=None):
        super().__init__(items, parent)
        self.items_originaux = items
        self.items_nettoyes = [nettoyer_texte(item) for item in items]
        
    def splitPath(self, path):
        texte_recherche = nettoyer_texte(path)
        resultats = [
            self.items_originaux[i] 
            for i, texte_propre in enumerate(self.items_nettoyes) 
            if texte_recherche in texte_propre
        ]
        self.setModel(QStringListModel(resultats))
        return [""]
    
# --- AJOUT : Fonction utilitaire pour récupérer la géométrie précise ---
def recuperer_geometrie_precise_ign(type_territoire: str, code_territoire: str, crs_cible: str = "EPSG:2154"):
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
        'SERVICE': 'WFS', 'VERSION': '2.0.0', 'REQUEST': 'GetFeature',
        'TYPENAMES': typename, 'OUTPUTFORMAT': 'application/json',
        'CQL_FILTER': cql_filter,
        'SRSNAME': crs_cible
    }

    try:
        response = requests.get(wfs_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        features = data.get('features', [])
        
        if features:
            # <-- MODIFICATION ICI : On passe le CRS dynamiquement
            gdf = geopandas.GeoDataFrame.from_features(features, crs=crs_cible) 
            if not gdf.empty:
                return gdf.geometry.iloc[0]
                
    except Exception as e:
        logger.error(f"Erreur WFS IGN (Géométrie précise) CRS {crs_cible}: {e}")
    
    return None

def determiner_contexte_spatial(code_insee=None, longitude=None) -> tuple[str, str]:
    """
    Routeur spatial : Détermine le CRS local et la zone géographique.
    Peut router via un code INSEE (mode Précis) ou une longitude WGS84 (mode Rectangle).
    Retourne (CRS, zone_geo)
    """
    if code_insee:
        code_dept = code_insee[:3] if code_insee.startswith('97') else code_insee[:2]
        if code_dept in ['971', '972']: return "EPSG:5490", "antilles"
        elif code_dept == '973': return "EPSG:2972", "guyane"
        elif code_dept == '974': return "EPSG:2975", "reunion"
        elif code_dept == '976': return "EPSG:4471", "mayotte"
        else: return "EPSG:2154", "metropole"
    
    if longitude is not None:
        if -65 < longitude < -59: return "EPSG:5490", "antilles"
        elif -55 < longitude < -51: return "EPSG:2972", "guyane"
        elif 55 < longitude < 56: return "EPSG:2975", "reunion"
        elif 45 < longitude < 46: return "EPSG:4471", "mayotte"
        else: return "EPSG:2154", "metropole"
        
    return "EPSG:2154", "metropole" # Fallback par défaut