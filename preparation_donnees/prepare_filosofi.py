import os
import zipfile
import tempfile
import glob
import shutil
import geopandas as gpd

FILOSOFI_DIR = r"P:\BiblioTechnique\MOBILITE\_Data\Filosofi - Carroyage INSEE 2019" 

def executer_mise_a_jour(zip_path): 
    try:
        import py7zr
    except ImportError:
        raise ImportError("La bibliothèque 'py7zr' manque. Tapez 'pip install py7zr' dans votre terminal.")

    print("--- Démarrage de l'extraction automatisée Filosofi (Séparation par Territoire) ---")

    if not zip_path or not os.path.exists(zip_path):
        raise FileNotFoundError(f"Le fichier ZIP source est introuvable : {zip_path}")

    # Dictionnaire de routage pour la préparation : [Mot_clé_Insee] -> [Nom_du_fichier_final]
    # reg02 = Martinique, reg04 = Réunion
    fichiers_cibles = {
        "met": "carreaux_200m_metropole.gpkg",
        "reg02": "carreaux_200m_antilles.gpkg",
        "reg04": "carreaux_200m_reunion.gpkg"
    }

    os.makedirs(FILOSOFI_DIR, exist_ok=True)

    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"1/4 Extraction du fichier ZIP principal : {os.path.basename(zip_path)}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        sevenz_files = glob.glob(os.path.join(temp_dir, "**", "*.7z"), recursive=True)
        if not sevenz_files:
            raise FileNotFoundError("Aucun fichier .7z n'a été trouvé à l'intérieur du ZIP de l'Insee.")
        
        print(f"2/4 Extraction du sous-fichier .7z (cela peut prendre un instant)...")
        with py7zr.SevenZipFile(sevenz_files[0], mode='r') as z:
            z.extractall(path=temp_dir)

        print("3/4 Recherche et transfert des fichiers géographiques cibles...")
        gpkg_files = glob.glob(os.path.join(temp_dir, "**", "*.gpkg"), recursive=True)
        
        fichiers_traites = 0
        for f in gpkg_files:
            nom_fichier = os.path.basename(f).lower()
            
            # On vérifie si ce fichier correspond à l'une de nos cibles
            for cle, nom_export in fichiers_cibles.items():
                if cle in nom_fichier:
                    # On le copie directement sans toucher au CRS !
                    # Il reste dans le système métrique original (2154, 5490 ou 2975)
                    chemin_export = os.path.join(FILOSOFI_DIR, nom_export)
                    shutil.copy2(f, chemin_export)
                    print(f"  -> {nom_export} sauvegardé avec succès.")
                    fichiers_traites += 1

        if fichiers_traites == 0:
            raise FileNotFoundError("Aucun fichier .gpkg cible n'a été trouvé dans l'archive.")

    print("4/4 Nettoyage de l'archive ZIP source...")
    os.remove(zip_path)
    
    print("-> Mise à jour de Filosofi terminée ! Fichiers régionaux prêts.")