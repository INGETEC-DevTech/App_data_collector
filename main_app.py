# main_app.py
print("Démarrage du programme...")
import sys
import os
import importlib
import inspect
import re
from PyQt6.QtWidgets import QApplication
from PyQt6.QtGui import QFontDatabase

import core.config as config
from core.logger_config import logger

logger.info("=========================================")
logger.info("Démarrage de l'application...")
logger.info("=========================================")

os.environ['QTWEBENGINE_REMOTE_DEBUGGING'] = '9223'

from data_sources.base_source import SourceDeDonneesBase
from gui.main_window import MainWindow

def resource_path(relative_path):
    """ Retourne le chemin absolu vers la ressource, compatible PyInstaller """
    try:
        # PyInstaller crée un dossier temporaire et stocke le chemin dans _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

def load_and_configure_data_sources(sources_package_name="data_sources") -> list[SourceDeDonneesBase]:
    """
    Charge dynamiquement toutes les sources de données, trouve leur configuration
    associée par convention de nommage, et les instancie.
    """
    data_sources_instances = []
    project_root = os.path.dirname(os.path.abspath(__file__))
    sources_dir = os.path.join(project_root, sources_package_name)

    if not os.path.exists(sources_dir):
        logger.error(f"Dossier des sources de données introuvable: '{sources_dir}'")
        return []

    # Lister les fichiers se terminant par _source.py pour la détection
    for filename in os.listdir(sources_dir):
        if filename.endswith('_source.py')and filename != 'base_source.py':
            module_name = filename[:-3]
            try:
                module = importlib.import_module(f"{sources_package_name}.{module_name}")
                # Transformer le nom de fichier en nom de classe (ex: 'bd_topo_source' -> 'BdTopoSource')
                expected_class_name = ''.join(word.capitalize() for word in module_name.split('_'))

                if hasattr(module, expected_class_name):
                    member_obj = getattr(module, expected_class_name)
                    if inspect.isclass(member_obj) and issubclass(member_obj, SourceDeDonneesBase) and member_obj is not SourceDeDonneesBase:
                        # Déduire le nom du bloc de config à partir du nom du module
                        config_block_name = module_name.upper() + "_CONFIG"
                        
                        if hasattr(config, config_block_name):
                            config_block = getattr(config, config_block_name)
                            # Instancier la classe en lui injectant sa configuration
                            instance = member_obj(config=config_block)
                            data_sources_instances.append(instance)
                        else:
                            logger.warning(f"Classe '{expected_class_name}' trouvée mais le bloc '{config_block_name}' manque dans config.py.")
                else:
                    logger.warning(f"Fichier '{filename}' trouvé mais la classe '{expected_class_name}' est introuvable.")
            except Exception as e:
                logger.error(f"Erreur lors du chargement de la source {module_name}: {e}")
    
    return data_sources_instances

if __name__ == "__main__":
    app = QApplication(sys.argv)

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    style_path = os.path.join(BASE_DIR, "assets/style.qss")

    # --- CHARGEMENT DE LA POLICE ---
    fonts_dir = os.path.join(BASE_DIR, "fonts")
    if os.path.exists(fonts_dir):
        for font_file in os.listdir(fonts_dir):
            if font_file.endswith('.ttf'):
                QFontDatabase.addApplicationFont(os.path.join(fonts_dir, font_file))
    
    # --- CHARGEMENT DU STYLE QSS ---
    try:
        with open(style_path, "r", encoding="utf-8") as f:
            app.setStyleSheet(f.read())
    except FileNotFoundError:
        logger.warning("Fichier style.qss non trouvé. Utilisation du style par défaut.")
    
    available_sources = load_and_configure_data_sources()
    if not available_sources:
        logger.error("Aucune source de données n'a pu être chargée. Vérifiez 'data_sources' et 'config.py'.")

    default_path = config.DEFAULT_EXPORT_PATH if hasattr(config, 'DEFAULT_EXPORT_PATH') else None

    window = MainWindow(loaded_data_sources=available_sources, default_export_path=default_path)
    window.show()
    sys.exit(app.exec())
