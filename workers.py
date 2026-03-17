from PyQt6.QtCore import QThread, pyqtSignal
import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__))     


class CollectorWorker(QThread):
    progress_signal = pyqtSignal(str)
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
        """Passe le drapeau d'annulation à Vrai."""
        self._is_cancelled = True

    def is_cancelled(self):
        """Retourne l'état du drapeau."""
        return self._is_cancelled

    def run(self):
        try:
            self.progress_signal.emit(f"\n[SOURCE] {self.data_source.nom_source}")
            collect_options_with_log = self.options_obj.copy() if self.options_obj is not None else {}
            collect_options_with_log["log_callback"] = self.progress_signal.emit
            collect_options_with_log["progress_callback"] = self.step_progress_signal.emit
            
            # --- NOUVEAU : On transmet la fonction de vérification d'annulation à la source ---
            collect_options_with_log["is_cancelled_callback"] = self.is_cancelled
            
            succes, message_global = self.data_source.collecter_donnees(
                self.export_dir, self.bbox_obj, collect_options_with_log)
            
            # On vérifie à la fin si l'opération a été annulée en cours de route
            if self._is_cancelled:
                self.finished_signal.emit(False, "🛑 Collecte annulée par l'utilisateur.")
            else:
                self.finished_signal.emit(succes, message_global)
                
        except Exception as e:
            import traceback
            error_msg = f"Erreur critique durant la collecte : {e}\n{traceback.format_exc()}"
            self.progress_signal.emit(error_msg); self.finished_signal.emit(False, error_msg)

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
    progress_signal = pyqtSignal(str)
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
            self.progress_signal.emit(f"Démarrage de la mise à jour pour {self.source_nom}...")
            recipe_type = self.recipe.get("type")
            
            # On récupère le dossier cible (uniquement si c'est un chemin unique, ex: BPE ou Filosofi)
            if isinstance(self.destination_path, str):
                target_dir = os.path.dirname(self.destination_path)
                os.makedirs(target_dir, exist_ok=True)

            if recipe_type == "simple_copy":
                
                # --- NOUVEAU : GESTION DES FICHIERS MULTIPLES (Ex: Flux Mobilité) ---
                if isinstance(self.destination_path, dict):
                    self.progress_signal.emit("Mise à jour multiple détectée...")
                    
                    # On associe chaque fichier sélectionné par l'utilisateur à sa destination
                    for i, (cle, dest_file) in enumerate(self.destination_path.items()):
                        src_file = self.selected_files_paths[i]
                        target_dir = os.path.dirname(dest_file)
                        os.makedirs(target_dir, exist_ok=True)
                        
                        if os.path.abspath(src_file) != os.path.abspath(dest_file):
                            self.progress_signal.emit(f"Copie de {os.path.basename(src_file)} vers {target_dir}...")
                            shutil.copy2(src_file, dest_file)
                        else:
                            self.progress_signal.emit(f"Le fichier '{os.path.basename(dest_file)}' est déjà à jour.")
                            
                    self.finished_signal.emit(True, "Tous les fichiers ont été mis à jour avec succès !")

                # --- ANCIENNE LOGIQUE : UN SEUL FICHIER (Ex: BPE) ---
                else:
                    src_file = self.selected_files_paths[0]
                    target_dir = os.path.dirname(self.destination_path)
                    os.makedirs(target_dir, exist_ok=True)
                    
                    if os.path.abspath(src_file) == os.path.abspath(self.destination_path):
                        self.progress_signal.emit("Le fichier est déjà au bon endroit sur le réseau. Copie ignorée.")
                    else:
                        self.progress_signal.emit(f"Copie du fichier en cours vers {target_dir}...")
                        shutil.copy2(src_file, self.destination_path)
                        
                    self.finished_signal.emit(True, "Fichier validé et mis à jour avec succès sur le réseau.")

            elif recipe_type == "preprocessing":
                self.progress_signal.emit("Vérification des fichiers bruts...")
                
                for src_file in self.selected_files_paths:
                    dest_file = os.path.join(target_dir, os.path.basename(src_file))
                    
                    # --- NOUVEAU : On ne copie que si c'est nécessaire ---
                    if os.path.abspath(src_file) != os.path.abspath(dest_file):
                        self.progress_signal.emit(f"Copie de {os.path.basename(src_file)} vers le serveur...")
                        shutil.copy2(src_file, dest_file)
                    else:
                        self.progress_signal.emit(f"'{os.path.basename(src_file)}' est déjà sur le serveur. Copie ignorée.")
                
                script_name = self.recipe.get("script_to_run")
                self.progress_signal.emit("Fichiers prêts. Lancement du prétraitement (~5 à 10 min)...")
                
                # Lancement dynamique du bon script
                if script_name == "prepare_bpe_local_to_network":
                    prep_dir = os.path.join(BASE_DIR, 'preparation_donnees')
                    if prep_dir not in sys.path: sys.path.append(prep_dir)
                    
                    from preparation_donnees import prepare_bpe, prepare_filosofi
                    import importlib
                    importlib.reload(prepare_bpe) # Recharge le script au cas où tu l'as modifié
                    prepare_bpe.prepare_bpe_local_to_network()
                
                elif script_name == "prepare_filosofi":
                    from preparation_donnees import prepare_filosofi
                    import importlib
                    prep_dir = os.path.join(BASE_DIR, 'preparation_donnees')
                    if prep_dir not in sys.path: sys.path.append(prep_dir)
                    importlib.reload(prepare_filosofi)
                    prepare_filosofi.executer_mise_a_jour()
                
                self.finished_signal.emit(True, "Prétraitement terminé et base consolidée mise à jour avec succès.")
            
            else:
                self.finished_signal.emit(False, f"Type de recette inconnu : {recipe_type}")

        except PermissionError:
            self.finished_signal.emit(False, "Un fichier est bloqué. Fermez Excel, QGIS ou tout autre logiciel utilisant ces données, puis réessayez.")
        except Exception as e:
            self.finished_signal.emit(False, f"Erreur critique lors de la mise à jour : {e}") 