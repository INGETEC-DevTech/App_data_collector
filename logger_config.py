# logger_config.py
import logging
import sys
import os
from PyQt6.QtCore import QObject, pyqtSignal

# ==============================================================================
# 1. CRÉATION DU VÉHICULE DE TRANSPORT (SIGNAL PYQT)
# ==============================================================================
class LogEmitter(QObject):
    """
    Un simple objet PyQt qui possède un signal.
    Il va transporter le texte du message et son niveau de gravité (INFO, ERROR, etc.)
    vers l'interface graphique.
    """
    # Ce signal envoie : (texte_du_message, niveau_de_log_en_entier)
    log_signal = pyqtSignal(str, int)

# ==============================================================================
# 2. CRÉATION DE L'ÉCOUTEUR (HANDLER) POUR LE MODULE LOGGING
# ==============================================================================
class PyQtLogHandler(logging.Handler):
    """
    Ce Handler personnalisé "écoute" le module de log standard de Python.
    Dès qu'un log.info() ou log.error() est appelé n'importe où dans le code,
    ce Handler l'attrape et l'émet via notre LogEmitter.
    """
    def __init__(self, emitter):
        super().__init__()
        self.emitter = emitter

    def emit(self, record):
        # On formate le message (pour qu'il respecte le format défini plus bas)
        msg = self.format(record)
        # On envoie le signal à PyQt !
        self.emitter.log_signal.emit(msg, record.levelno)

# ==============================================================================
# 3. CONFIGURATION CENTRALE DU LOGGER
# ==============================================================================
def setup_logger():
    # On crée notre logger principal nommé "AppCollector"
    main_logger = logging.getLogger("AppCollector")
    main_logger.setLevel(logging.DEBUG) # On capte tout à la source

    # Sécurité : on nettoie si la fonction est appelée plusieurs fois
    if main_logger.hasHandlers():
        main_logger.handlers.clear()

    # Formats des messages
    # Format fichier/console (très détaillé) : 2024-03-12 14:30:00 - INFO - [SOURCE] Démarrage...
    detailed_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    # Format UI (épuré) : L'heure sera rajoutée par l'interface elle-même
    ui_formatter = logging.Formatter('%(message)s')

    # --- CANAL 1 : FICHIER CACHÉ (app.log) ---
    # Parfait pour le débogage. Il contiendra TOUT, même les DEBUG.
    log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    main_logger.addHandler(file_handler)

    # --- CANAL 2 : CONSOLE (Terminal) ---
    # Pour vous, pendant le développement.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(detailed_formatter)
    main_logger.addHandler(console_handler)

    # --- CANAL 3 : INTERFACE GRAPHIQUE (PyQt) ---
    # On connecte notre handler magique.
    my_log_emitter = LogEmitter()
    gui_handler = PyQtLogHandler(my_log_emitter)
    # On ne pollue pas l'UI avec du DEBUG. On n'envoie que INFO, WARNING, ERROR, CRITICAL
    gui_handler.setLevel(logging.INFO) 
    gui_handler.setFormatter(ui_formatter)
    main_logger.addHandler(gui_handler)

    return main_logger, my_log_emitter

# ==============================================================================
# 4. INSTANCIATION GLOBALE
# ==============================================================================
# On crée les instances une seule fois ici.
# Dans n'importe quel autre fichier, il suffira de faire :
# from logger_config import logger
logger, log_emitter = setup_logger()