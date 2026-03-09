import geopandas as gpd
import os
import json

# --- Paramètres ---
ASSETS_DIR = "assets"
FILES_TO_PROCESS = {
    "communes.geojson": {
        "output": "communes_simplifie.geojson",
        "simplify_tolerance": 500 # 
    },
    "epci.geojson": {
        "output": "epci_simplifie.geojson", # On simplifie aussi les EPCI pour la performance
        "simplify_tolerance": 500
    }
}
ID_FIELD = "codgeo"

# --- Script ---
print("Début de la préparation des fichiers GeoJSON...")

for input_file, config in FILES_TO_PROCESS.items():
    input_path = os.path.join(ASSETS_DIR, input_file)
    output_path = os.path.join(ASSETS_DIR, config["output"])
    tolerance = config["simplify_tolerance"]

    print(f"\nTraitement du fichier : {input_path}...")
    if not os.path.exists(input_path):
        print(f"-> ERREUR : Fichier non trouvé. Ignoré.")
        continue

    try:
        # 1. Lecture du fichier
        gdf = gpd.read_file(input_path)

        # 2. Simplification
        print(f"-> Simplification avec une tolérance de {tolerance}m...")
        gdf_proj = gdf.to_crs("EPSG:2154")
        gdf_proj['geometry'] = gdf_proj.geometry.simplify(tolerance=tolerance)
        gdf_final = gdf_proj.to_crs(gdf.crs)

        # 3. Conversion en dictionnaire GeoJSON pour manipulation
        print("-> Ajout du champ 'id' requis par Folium...")
        geojson_dict = json.loads(gdf_final.to_json())

        # 4. Ajout du champ 'id' à la racine de chaque "feature"
        for feature in geojson_dict['features']:
            if ID_FIELD in feature['properties']:
                feature['id'] = feature['properties'][ID_FIELD]
            else:
                print(f"AVERTISSEMENT: le champ d'ID '{ID_FIELD}' n'a pas été trouvé dans une des formes.")

        # 5. Sauvegarde du fichier final corrigé
        print(f"-> Sauvegarde vers : {output_path}...")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(geojson_dict, f)
        
        print(f"-> Fichier '{config['output']}' traité avec succès.")

    except Exception as e:
        print(f"\n-> Une erreur est survenue lors du traitement de {input_file} : {e}")

print("\nPréparation terminée !")