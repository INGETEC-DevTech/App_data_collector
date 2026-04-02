import os
import zipfile
import tempfile
import glob
import pandas as pd
import geopandas as gpd

# --- CONFIGURATION ---
FILOSOFI_DIR = r"P:\BiblioTechnique\MOBILITE\_Data\Filosofi - Carroyage INSEE 2019" 
# Nouveau nom de fichier pour refléter la couverture totale
TARGET_GPKG_NAME = "carreaux_200m_france_entiere.gpkg" 

def executer_mise_a_jour(zip_path): 
    try:
        import py7zr
    except ImportError:
        raise ImportError("La bibliothèque 'py7zr' manque. Tapez 'pip install py7zr' dans votre terminal.")

    print("--- Démarrage de l'extraction automatisée Filosofi (Métropole + DROM) ---")

    if not zip_path or not os.path.exists(zip_path):
        raise FileNotFoundError(f"Le fichier ZIP source est introuvable : {zip_path}")

    print(f"1/5 Extraction du fichier ZIP principal : {os.path.basename(zip_path)}...")

    with tempfile.TemporaryDirectory() as temp_dir:
        # 1. Extraire le ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # 2. Trouver et extraire le fichier .7z caché
        sevenz_files = glob.glob(os.path.join(temp_dir, "**", "*.7z"), recursive=True)
        if not sevenz_files:
            raise FileNotFoundError("Aucun fichier .7z n'a été trouvé à l'intérieur du ZIP de l'Insee.")
        
        print(f"2/5 Extraction du fichier .7z (cela peut prendre un instant)...")
        with py7zr.SevenZipFile(sevenz_files[0], mode='r') as z:
            z.extractall(path=temp_dir)

        # 3. Chercher les fichiers cibles (Métropole, reg02, reg04)
        print("3/5 Recherche des fichiers géographiques cibles...")
        gpkg_files = glob.glob(os.path.join(temp_dir, "**", "*.gpkg"), recursive=True)
        
        fichiers_a_traiter = []
        # On cible explicitement les mots clés des fichiers qui nous intéressent
        mots_cles = ["met", "metropole", "reg02", "reg04"]
        
        for f in gpkg_files:
            nom_fichier = os.path.basename(f).lower()
            if any(mot in nom_fichier for mot in mots_cles):
                fichiers_a_traiter.append(f)

        if not fichiers_a_traiter:
            raise FileNotFoundError("Aucun fichier .gpkg cible n'a été trouvé dans l'archive.")

        # 4. Lecture, conversion des coordonnées et fusion
        print(f"4/5 Fusion et uniformisation des coordonnées (WGS84) pour {len(fichiers_a_traiter)} fichiers...")
        gdfs = []
        for fichier in fichiers_a_traiter:
            nom = os.path.basename(fichier)
            print(f"  -> Traitement de {nom}...")
            
            # Lecture optimisée
            gdf = gpd.read_file(fichier, engine="pyogrio")
            
            # MAGIE ICI : On convertit tout le monde dans le même système de coordonnées GPS (WGS84)
            if gdf.crs != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")
            
            gdfs.append(gdf)

        # On empile tous les tableaux pour n'en faire qu'un seul
        print("  -> Assemblage final des territoires...")
        gdf_final = pd.concat(gdfs, ignore_index=True)
        gdf_final = gpd.GeoDataFrame(gdf_final, geometry='geometry', crs="EPSG:4326")

        # 5. Export du super-fichier vers le lecteur réseau P:/
        final_path = os.path.join(FILOSOFI_DIR, TARGET_GPKG_NAME)
        print(f"5/5 Création du GeoPackage unifié vers {final_path} (peut prendre quelques minutes)...")
        os.makedirs(FILOSOFI_DIR, exist_ok=True)
        
        # On sauvegarde avec un index spatial pour garantir un affichage très rapide sur l'app
        gdf_final.to_file(final_path, driver="GPKG", engine="pyogrio")

    # 6. Nettoyage de la grosse archive source
    print("Nettoyage de l'archive ZIP source...")
    os.remove(zip_path)
    
    print("-> Mise à jour de Filosofi terminée avec succès !")

if __name__ == "__main__":
    pass