from PyQt6.QtCore import QThread, pyqtSignal
import os
import shutil
import sys
import requests
from core.logger_config import logger
from core.utils import get_resource_path

# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = get_resource_path("")

class CollectorWorker(QThread):
    step_progress_signal = pyqtSignal(int, int)
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
            logger.exception(f"Erreur critique durant la collecte de {self.data_source.nom_source}")
            self.finished_signal.emit(False, f"Erreur critique: {e}")

class SourceValidatorWorker(QThread):
    validation_result_signal = pyqtSignal(str, bool, str)
    finished_signal = pyqtSignal()

    def __init__(self, sources):
        super().__init__()
        self.sources = sources

    def run(self):
        for source in self.sources:
            try:
                success, message = source.valider_lien()
                self.validation_result_signal.emit(source.nom_source, success, message)
            except Exception as e:
                self.validation_result_signal.emit(source.nom_source, False, f"Erreur critique : {e}")
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
        try:
            logger.info(f"Démarrage de la mise à jour pour {self.source_nom}...")
            recipe_type = self.recipe.get("type")
            
            # Définition du target_dir
            if isinstance(self.destination_path, str):
                target_dir = os.path.dirname(self.destination_path)
            else:
                first_path = list(self.destination_path.values())[0]
                target_dir = os.path.dirname(first_path)
            
            os.makedirs(target_dir, exist_ok=True)

            # --- CAS 1 : SIMPLE COPY ---
            if recipe_type == "simple_copy":
                if isinstance(self.destination_path, dict):
                    for i, (cle, dest_file) in enumerate(self.destination_path.items()):
                        src_file = self.selected_files_paths[i]
                        if os.path.abspath(src_file) != os.path.abspath(dest_file):
                            shutil.copy2(src_file, dest_file)
                    self.finished_signal.emit(True, "Mise à jour multiple réussie.")
                else:
                    src_file = self.selected_files_paths[0]
                    if os.path.abspath(src_file) != os.path.abspath(self.destination_path):
                        shutil.copy2(src_file, self.destination_path)
                    self.finished_signal.emit(True, "Fichier mis à jour.")

            # --- CAS 2 : PREPROCESSING ---
            elif recipe_type == "preprocessing":
                logger.info("Vérification des fichiers bruts...")
                fichiers_copies = []
                for src_file in self.selected_files_paths:
                    dest_file = os.path.join(target_dir, os.path.basename(src_file))
                    fichiers_copies.append(dest_file)
                    if os.path.abspath(src_file) != os.path.abspath(dest_file):
                        shutil.copy2(src_file, dest_file)
                
                script_name = self.recipe.get("script_to_run")
                logger.info(f"Lancement du prétraitement : {script_name}")
                
                # Ajout du dossier préparation au path
                prep_dir = os.path.join(BASE_DIR, 'preparation_donnees')
                if prep_dir not in sys.path: sys.path.append(prep_dir)
                import importlib

                if script_name == "prepare_flux_mobilite":
                    from preparation_donnees import prepare_flux_mobilite
                    sources = {"travail": fichiers_copies[0], "etude": fichiers_copies[1]}
                    prepare_flux_mobilite.executer_mise_a_jour(sources, self.destination_path)

                elif script_name == "prepare_bpe_local_to_network":
                    from preparation_donnees import prepare_bpe
                    importlib.reload(prepare_bpe)
                    f_parquet = next((f for f in fichiers_copies if f.endswith('.parquet')), None)
                    f_csv = next((f for f in fichiers_copies if f.endswith('.csv')), None)
                    f_excel = next((f for f in fichiers_copies if f.endswith('.xlsx')), None)
                    prepare_bpe.prepare_bpe_local_to_network(f_parquet, f_csv, f_excel)

                elif script_name == "prepare_filosofi":
                    from preparation_donnees import prepare_filosofi
                    importlib.reload(prepare_filosofi)
                    prepare_filosofi.executer_mise_a_jour(fichiers_copies[0])

                elif script_name == "prepare_bnac":
                    from preparation_donnees import prepare_bnac
                    importlib.reload(prepare_bnac)
                    prepare_bnac.executer_mise_a_jour(fichiers_copies[0])
                
                elif script_name == "prepare_carte_scolaire":
                    from preparation_donnees import prepare_carte_scolaire
                    importlib.reload(prepare_carte_scolaire)
                    f_parquet = next((f for f in fichiers_copies if f.endswith(('.parquet', '.geoparquet'))), None)
                    f_csv = next((f for f in fichiers_copies if f.endswith('.csv')), None)
                    if not f_parquet or not f_csv:
                        raise ValueError("Fichiers Parquet ou CSV manquants.")
                    prepare_carte_scolaire.executer_mise_a_jour(f_parquet, f_csv)
                
                self.finished_signal.emit(True, "Prétraitement terminé avec succès.")
            
            else:
                self.finished_signal.emit(False, f"Type de recette inconnu : {recipe_type}")

        except PermissionError:
            self.finished_signal.emit(False, "Fichier bloqué. Fermez Excel ou QGIS.")
        except Exception as e:
            logger.exception("Erreur lors de la mise à jour")
            self.finished_signal.emit(False, f"Erreur : {e}")

class IgnFetcherWorker(QThread):
    result_signal = pyqtSignal(bool, dict)

    def __init__(self, tipo, code_recherche, parent=None):
        super().__init__(parent)
        self.tipo = tipo
        self.code_recherche = code_recherche

    def run(self):
        try:
            layer_name = 'BDTOPO_V3:commune' if self.tipo == 'Commune' else 'BDTOPO_V3:epci'
            filter_prop = 'code_insee' if self.tipo == 'Commune' else 'code_siren'
            params = {
                'SERVICE': 'WFS', 'VERSION': '2.0.0', 'REQUEST': 'GetFeature',
                'TYPENAMES': layer_name, 'OUTPUTFORMAT': 'application/json',
                'CQL_FILTER': f"{filter_prop}='{self.code_recherche}'", 'SRSNAME': 'EPSG:4326'
            }
            response = requests.get("https://data.geopf.fr/wfs/ows", params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get('features'):
                self.result_signal.emit(True, data['features'][0])
            else:
                self.result_signal.emit(False, {})
        except Exception as e:
            logger.error(f"Erreur API IGN : {e}")
            self.result_signal.emit(False, {})