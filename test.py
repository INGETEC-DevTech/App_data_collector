import requests
import xml.etree.ElementTree as ET

url = "https://data.geopf.fr/wfs/ows"
params = {
    'SERVICE': 'WFS',
    'VERSION': '2.0.0',
    'REQUEST': 'GetCapabilities'
}

print("Interrogation de l'IGN en cours...")
response = requests.get(url, params=params)

if response.status_code == 200:
    # On lit le XML
    root = ET.fromstring(response.content)
    
    # On cherche toutes les couches (FeatureType)
    # L'espace de nom (namespace) du WFS 2.0.0 complique un peu la recherche, d'où ce format
    layers = []
    for feature_type in root.findall('.//{http://www.opengis.net/wfs/2.0}FeatureType'):
        name_tag = feature_type.find('{http://www.opengis.net/wfs/2.0}Name')
        title_tag = feature_type.find('{http://www.opengis.net/wfs/2.0}Title')
        
        if name_tag is not None:
            # On ne garde que les couches de la BD TOPO pour y voir clair
            if "BDTOPO" in name_tag.text:
                layers.append(name_tag.text)
    
    print(f"\n✅ {len(layers)} couches trouvées dans la BD TOPO :")
    for layer in sorted(layers):
        print(f" - {layer}")
else:
    print("Erreur de connexion à l'IGN")