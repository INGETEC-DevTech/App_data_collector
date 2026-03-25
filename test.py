import geopandas as gpd

chemin_fichier = r"assets\communes.geojson"

try:
    # On ne lit que la première ligne pour que ce soit instantané
    gdf = gpd.read_file(chemin_fichier, rows=1)
    print("\n=== COLONNES DE TON FICHIER COMMUNE ===")
    print(gdf.columns.tolist())
    print("========================================\n")
except Exception as e:
    print(f"Erreur : {e}")