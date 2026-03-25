from PyQt6.QtCore import QThread, pyqtSignal
import os
from logger_config import logger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))     


class CollectorWorker(QThread):
    step_progress_signal = pyqtSignal(int, int) # (valeur_actuelle, valeur_totale)
    finished_signal = pyqtSignal(bool, str)

    def __init__(self, parent, data_source_instance, export_dir, bbox_obj, options_obj):
        super().__init__(parent)
        self.data_source = data_source_instance
        self.export_dir = export_dir
        self.bbox_obj = bbox_obj
        self.options_obj = options_obj
        self._is_cancelled = False
    
    def cancel(self):
        self._is_cancelled = True

    def is_cancelled(self):
        return self._is_cancelled

    def run(self):
        try:
            logger.info(f"\n[SOURCE] {self.data_source.nom_source}")
            collect_options_with_log = self.options_obj.copy() if self.options_obj is not None else {}
            
            # MAGIQUE : On passe directement le logger à la source !
            collect_options_with_log["log_callback"] = logger.info 
            collect_options_with_log["progress_callback"] = self.step_progress_signal.emit
            collect_options_with_log["is_cancelled_callback"] = self.is_cancelled
            
            succes, message_global = self.data_source.collecter_donnees(
                self.export_dir, self.bbox_obj, collect_options_with_log)
            
            if self._is_cancelled:
                self.finished_signal.emit(False, "Collecte annulée par l'utilisateur.")
            else:
                self.finished_signal.emit(succes, message_global)
                
        except Exception as e:
            # logger.exception enregistre automatiquement l'erreur ET le traceback dans app.log !
            logger.exception(f"Erreur critique durant la collecte de {self.data_source.nom_source}")
            self.finished_signal.emit(False, f"Erreur critique: {e}")

class SourceValidatorWorker(QThread):
    # Signaux: nom_source, succes (bool), message
    validation_result_signal = pyqtSignal(str, bool, str)
    finished_signal = pyqtSignal()

    def __init__(self, sources):
        super().__init__()
        self.sources = sources

    def run(self):
        for source in self.sources:
            try:
                # On appelle la méthode valider_lien de chaque source
                success, message = source.valider_lien()
                self.validation_result_signal.emit(source.nom_source, success, message)
            except Exception as e:
                self.validation_result_signal.emit(source.nom_source, False, f"Erreur critique lors de la vérification : {e}")
        self.finished_signal.emit()

class UpdaterWorker(QThread):
    finished_signal = pyqtSignal(bool, str)

    def __init__(self, source_nom, recipe, selected_files_paths, destination_path):
        super().__init__()
        self.source_nom = source_nom
        self.recipe = recipe
        self.selected_files_paths = selected_files_paths
        self.destination_path = destination_path

    def run(self):
        import shutil
        import sys
        import os
        
        try:
            logger.info(f"Démarrage de la mise à jour pour {self.source_nom}...")
            recipe_type = self.recipe.get("type")
            
            # On récupère le dossier cible (uniquement si c'est un chemin unique, ex: BPE ou Filosofi)
            if isinstance(self.destination_path, str):
                target_dir = os.path.dirname(self.destination_path)
                os.makedirs(target_dir, exist_ok=True)

            if recipe_type == "simple_copy":
                
                # --- NOUVEAU : GESTION DES FICHIERS MULTIPLES (Ex: Flux Mobilité) ---
                if isinstance(self.destination_path, dict):
                    logger.info("Mise à jour multiple détectée...")
                    
                    # On associe chaque fichier sélectionné par l'utilisateur à sa destination
                    for i, (cle, dest_file) in enumerate(self.destination_path.items()):
                        src_file = self.selected_files_paths[i]
                        target_dir = os.path.dirname(dest_file)
                        os.makedirs(target_dir, exist_ok=True)
                        
                        if os.path.abspath(src_file) != os.path.abspath(dest_file):
                            logger.info(f"Copie de {os.path.basename(src_file)} vers {target_dir}...")
                            shutil.copy2(src_file, dest_file)
                        else:
                            logger.info(f"Le fichier '{os.path.basename(dest_file)}' est déjà à jour.")
                            
                    self.finished_signal.emit(True, "Tous les fichiers ont été mis à jour avec succès !")

                # --- ANCIENNE LOGIQUE : UN SEUL FICHIER (Ex: BPE) ---
                else:
                    src_file = self.selected_files_paths[0]
                    target_dir = os.path.dirname(self.destination_path)
                    os.makedirs(target_dir, exist_ok=True)
                    
                    if os.path.abspath(src_file) == os.path.abspath(self.destination_path):
                        logger.info("Le fichier est déjà au bon endroit sur le réseau. Copie ignorée.")
                    else:
                        logger.info(f"Copie du fichier en cours vers {target_dir}...")
                        shutil.copy2(src_file, self.destination_path)
                        
                    self.finished_signal.emit(True, "Fichier validé et mis à jour avec succès sur le réseau.")

            elif recipe_type == "preprocessing":
                logger.info("Vérification des fichiers bruts...")
                
                fichiers_copies = []

                for src_file in self.selected_files_paths:
                    dest_file = os.path.join(target_dir, os.path.basename(src_file))
                    fichiers_copies.append(dest_file)
                    
                    # --- NOUVEAU : On ne copie que si c'est nécessaire ---
                    if os.path.abspath(src_file) != os.path.abspath(dest_file):
                        logger.info(f"Copie de {os.path.basename(src_file)} vers le serveur...")
                        shutil.copy2(src_file, dest_file)
                    else:
                        logger.info(f"'{os.path.basename(src_file)}' est déjà sur le serveur. Copie ignorée.")
                
                script_name = self.recipe.get("script_to_run")
                logger.info("Fichiers prêts. Lancement du prétraitement (~5 à 10 min)...")
                
                # Lancement dynamique du bon script
                if script_name == "prepare_bpe_local_to_network":
                    prep_dir = os.path.join(BASE_DIR, 'preparation_donnees')
                    if prep_dir not in sys.path: sys.path.append(prep_dir)
                    
                    from preparation_donnees import prepare_bpe, prepare_filosofi
                    import importlib
                    importlib.reload(prepare_bpe) # Recharge le script au cas où tu l'as modifié

                    # On identifie qui est qui grâce aux extensions
                    fichier_parquet = next((f for f in fichiers_copies if f.endswith('.parquet')), None)
                    fichier_csv = next((f for f in fichiers_copies if f.endswith('.csv')), None)
                    fichier_excel = next((f for f in fichiers_copies if f.endswith('.xlsx')), None)
                    
                    # On lance la machine en lui donnant les 3 fichiers exacts
                    prepare_bpe.prepare_bpe_local_to_network(fichier_parquet, fichier_csv, fichier_excel)

                
                elif script_name == "prepare_filosofi":
                    from preparation_donnees import prepare_filosofi
                    import importlib
                    prep_dir = os.path.join(BASE_DIR, 'preparation_donnees')
                    if prep_dir not in sys.path: sys.path.append(prep_dir)
                    importlib.reload(prepare_filosofi)
                    prepare_filosofi.executer_mise_a_jour(dest_file)

                elif script_name == "prepare_bnac":
                    from preparation_donnees import prepare_bnac
                    import importlib
                    prep_dir = os.path.join(BASE_DIR, 'preparation_donnees')
                    if prep_dir not in sys.path: sys.path.append(prep_dir)
                    importlib.reload(prepare_bnac)
                    prepare_bnac.executer_mise_a_jour(dest_file)
                
                elif script_name == "prepare_carte_scolaire":
                    from preparation_donnees import prepare_carte_scolaire
                    import importlib
                    prep_dir = os.path.join(BASE_DIR, 'preparation_donnees')
                    if prep_dir not in sys.path: sys.path.append(prep_dir)
                    importlib.reload(prepare_carte_scolaire)

                    # Identification des fichiers par leur extension
                    fichier_parquet = next((f for f in fichiers_copies if f.endswith(('.parquet', '.geoparquet'))), None)
                    fichier_csv = next((f for f in fichiers_copies if f.endswith('.csv')), None)

                    if not fichier_parquet or not fichier_csv:
                        raise ValueError("Fichiers manquants : Veuillez fournir le fichier Parquet ET le fichier CSV.")

                    # Lancement du script
                    prepare_carte_scolaire.executer_mise_a_jour(fichier_parquet, fichier_csv)
                
                self.finished_signal.emit(True, "Prétraitement terminé et base consolidée mise à jour avec succès.")
            
            else:
                self.finished_signal.emit(False, f"Type de recette inconnu : {recipe_type}")

        except PermissionError:
            self.finished_signal.emit(False, "Un fichier est bloqué. Fermez Excel, QGIS ou tout autre logiciel utilisant ces données, puis réessayez.")
        except Exception as e:
            self.finished_signal.emit(False, f"Erreur critique lors de la mise à jour : {e}") 