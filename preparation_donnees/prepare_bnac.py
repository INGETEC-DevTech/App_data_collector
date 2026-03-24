import os
import geopandas as gpd

BNAC_DIR = r"P:\BiblioTechnique\MOBILITE\_Data\Base Nationale des Aménagements Cyclables"
TARGET_GPKG_NAME = "amenagements_cyclables_bnac.gpkg"

def executer_mise_a_jour(fichier_source):
    print(f"\n--- Démarrage de la conversion BNAC ---")
    print(f"1/3 Fichier reçu : {os.path.basename(fichier_source)}")

    # 2. Lecture optimisée
    print("2/3 Lecture du fichier avec pyogrio...")
    gdf = gpd.read_file(fichier_source, engine="pyogrio")

    # 3. Conversion en GeoPackage
    print("3/3 Création du GeoPackage avec index spatial...")
    chemin_destination = os.path.join(BNAC_DIR, TARGET_GPKG_NAME)
    gdf.to_file(chemin_destination, driver="GPKG", engine="pyogrio")

    # 4. Nettoyage du fichier exact qu'on vient de traiter
    print("Nettoyage du fichier GeoJSON source...")
    os.remove(fichier_source)

    print("-> Mise à jour de la BNAC terminée avec succès !")

if __name__ == "__main__":
    # Si lancé à la main pour tester (optionnel)
    pass