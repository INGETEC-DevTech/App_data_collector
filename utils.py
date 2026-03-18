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
def recuperer_geometrie_precise_ign(type_territoire: str, code_territoire: str):
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
        logger.error(f"Erreur WFS IGN (Géométrie précise) : {e}")
    
    return None