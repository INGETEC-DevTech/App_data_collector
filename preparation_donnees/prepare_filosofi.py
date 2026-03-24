import os
import zipfile
import tempfile
import shutil
import glob

# À ADAPTER : Mets ici le chemin du dossier où Filosofi est stocké sur P:/
FILOSOFI_DIR = r"P:\BiblioTechnique\MOBILITE\_Data\Filosofi - Carroyage INSEE 2019" 
TARGET_GPKG_NAME = "carreaux_200m_met.gpkg" 

# On demande le fichier exact en paramètre
def executer_mise_a_jour(zip_path): 
    try:
        import py7zr
    except ImportError:
        raise ImportError("La bibliothèque 'py7zr' manque. Tapez 'pip install py7zr' dans votre terminal.")

    print("--- Démarrage de l'extraction automatisée Filosofi ---")

    # On vérifie juste que le fichier qu'on nous donne existe bien.
    if not zip_path or not os.path.exists(zip_path):
        raise FileNotFoundError(f"Le fichier ZIP source est introuvable : {zip_path}")

    print(f"1/4 Extraction du fichier ZIP principal : {os.path.basename(zip_path)}...")

    # Création d'un dossier temporaire invisible qui se supprimera tout seul
    with tempfile.TemporaryDirectory() as temp_dir:
        # 2. Extraire le ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # 3. Trouver et extraire le fichier .7z caché à l'intérieur
        sevenz_files = glob.glob(os.path.join(temp_dir, "**", "*.7z"), recursive=True)
        if not sevenz_files:
            raise FileNotFoundError("Aucun fichier .7z n'a été trouvé à l'intérieur du ZIP de l'Insee.")
        
        print(f"2/4 Extraction du fichier .7z (cela peut prendre un instant)...")
        with py7zr.SevenZipFile(sevenz_files[0], mode='r') as z:
            z.extractall(path=temp_dir)

        # 4. Chercher le bon fichier .gpkg (Pattern Matching)
        print("3/4 Recherche du fichier métropole...")
        gpkg_files = glob.glob(os.path.join(temp_dir, "**", "*.gpkg"), recursive=True)
        
        target_file = None
        for f in gpkg_files:
            nom_fichier = os.path.basename(f).lower()
            if "met" in nom_fichier or "metropole" in nom_fichier:
                target_file = f
                break

        if not target_file:
            raise FileNotFoundError("Aucun fichier .gpkg contenant 'met' ou 'metropole' n'a été trouvé dans l'archive.")

        # 5. Déplacer et renommer le fichier final vers P:/
        final_path = os.path.join(FILOSOFI_DIR, TARGET_GPKG_NAME)
        print(f"4/4 Déplacement et renommage vers {final_path}...")
        shutil.copy2(target_file, final_path)

    # 6. Nettoyage : On supprime le gros ZIP brut de P:/ pour faire de la place
    print("Nettoyage de l'archive ZIP...")
    os.remove(zip_path)
    
    print("Mise à jour de Filosofi terminée avec succès !")

if __name__ == "__main__":
    executer_mise_a_jour()