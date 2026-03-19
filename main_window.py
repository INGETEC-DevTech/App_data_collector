import os
import json
from shapely.geometry import shape
from PyQt6.QtWidgets import (QMainWindow, QPushButton, QVBoxLayout, QHBoxLayout, QWidget,
                             QTextEdit, QListWidget, QListWidgetItem,
                             QLabel, QFileDialog, QLineEdit, 
                             QProgressBar, QSplitter)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QIcon, QFontMetrics
from PyQt6.QtWebEngineWidgets import QWebEngineView
from datetime import datetime
from shapely import geometry
from map_handler import MapManager, FOLIUM_AVAILABLE

from data_sources.base_source import SourceDeDonneesBase
from workers import SourceValidatorWorker, CollectorWorker, UpdaterWorker
from utils import CompleterIntelligent, recuperer_geometrie_precise_ign

from gui_module import (OverlaySearchWidget, SourceListItemWidget, 
                        LayerSelectionDialog, GenericOptionsDialog,
                        UpdateCenterDialog)
import logging
from logger_config import logger, log_emitter
import pyproj
from shapely.ops import transform
import geopandas as gpd
from shapely.geometry import box


BASE_DIR = os.path.dirname(os.path.abspath(__file__))     

class MainWindow(QMainWindow):
    def __init__(self, loaded_data_sources: list[SourceDeDonneesBase], default_export_path: str | None = None):
        super().__init__()
        self.setWindowTitle("Collecteur de Données INGETEC")
        #self.setGeometry(100, 100, 1400, 900)
        self.setMinimumSize(1200, 800)
        self.showMaximized()
        
        self.loaded_data_sources = loaded_data_sources
        self.current_source_config_options = {}
        self.export_directory = default_export_path
        self.perimeter_is_defined = False
        
        # Champs techniques (BBOX) conservés pour la logique interne
        self.min_x_edit = QLineEdit("855000"); self.min_y_edit = QLineEdit("6518000")
        self.max_x_edit = QLineEdit("857000"); self.max_y_edit = QLineEdit("6520000")
        self.crs_edit = QLineEdit("EPSG:2154")

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)

        # --- PANNEAU GAUCHE AVEC SPLITTER (Sources en haut, Logs en bas) ---
        left_container = QWidget()
        left_vbox = QVBoxLayout(left_container)

        self.splitter = QSplitter(Qt.Orientation.Vertical)
        # 1. On donne un peu plus d'épaisseur à la barre pour l'attraper facilement
        self.splitter.setHandleWidth(4)

        # 2. On lui donne un look sympa et visible (rappel du vert au survol)
        self.splitter.setStyleSheet("""
            QSplitter::handle:vertical {
                background-color: #e0e0e0; /* Gris très clair par défaut */
                border-top: 1px solid #dcdcdc;
                border-bottom: 1px solid #dcdcdc;
                margin: 0px 50px; /* La barre ne touche pas les bords, c'est plus moderne */
                border-radius: 2px;
            }
            QSplitter::handle:vertical:hover {
                background-color: #8bc34a; /* Devient vert au survol pour inciter au clic ! */
                border: none;
            }
        """)

        # Zone Sources et Export
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        top_layout.setContentsMargins(0, 0, 0, 10)
        
        # --- NOUVEAU : En-tête avec Titre, Mini-boutons ET Bouton de mise à jour ---
        header_layout = QHBoxLayout()
        sources_title = QLabel("Sources de Données")
        sources_title.setObjectName("titleLabel")
        
        # Création des mini-boutons
        self.btn_select_all = QPushButton("Tout cocher")
        self.btn_deselect_all = QPushButton("Tout décocher")
        
        # Style minimaliste (texte cliquable discret)
        mini_btn_style = """
            QPushButton { background-color: transparent; color: #7f8c8d; text-decoration: underline; border: none; font-size: 11px; padding: 2px 5px; }
            QPushButton:hover { color: #2c3e50; font-weight: bold; }
        """
        self.btn_select_all.setStyleSheet(mini_btn_style)
        self.btn_deselect_all.setStyleSheet(mini_btn_style)
        
        self.btn_select_all.clicked.connect(self.tout_cocher)
        self.btn_deselect_all.clicked.connect(self.tout_decocher)
        
        self.btn_open_update_center = QPushButton("Mise à jour des données")
        self.btn_open_update_center.setStyleSheet("""
            QPushButton { background-color: #3498db; color: white; font-weight: bold; border-radius: 5px; padding: 5px 10px; }
            QPushButton:hover { background-color: #2980b9; }
        """)
        self.btn_open_update_center.clicked.connect(self.ouvrir_centre_mise_a_jour)
        
        # Ajout dans l'en-tête
        header_layout.addWidget(sources_title)
        header_layout.addWidget(self.btn_select_all)
        header_layout.addWidget(QLabel("<span style='color:#bdc3c7;'>|</span>")) # Petit séparateur
        header_layout.addWidget(self.btn_deselect_all)
        header_layout.addStretch()
        header_layout.addWidget(self.btn_open_update_center)
        top_layout.addLayout(header_layout)

        
        self.sources_list_widget = QListWidget()
        top_layout.addWidget(self.sources_list_widget, 1)

        # Zone Export
        export_layout = QHBoxLayout()
        export_title = QLabel("Dossier d'Exportation"); export_title.setObjectName("titleLabel")
        self.export_dir_button = QPushButton()
        self.export_dir_button.setIcon(QIcon(os.path.join(BASE_DIR, 'icons', 'folder.svg')))
        self.export_dir_button.clicked.connect(self.select_export_directory)
        export_layout.addWidget(export_title); export_layout.addStretch(); export_layout.addWidget(self.export_dir_button)
        top_layout.addLayout(export_layout)
        
        self.export_dir_label = QLabel(f"<i>{self.export_directory or 'Aucun dossier sélectionné'}</i>")
        top_layout.addWidget(self.export_dir_label)

        # =====================================================================
        # ZONE DES BOUTONS D'ACTION
        # =====================================================================
        action_buttons_layout = QHBoxLayout() # Crée une ligne horizontale

        # 1. Le bouton Lancer
        self.collect_button = QPushButton(" LANCER LA COLLECTE")
        self.collect_button.setObjectName("launchButton")
        self.collect_button.setIcon(QIcon(os.path.join(BASE_DIR, 'icons', 'play.svg')))
        self.collect_button.clicked.connect(self.lancer_collecte_multiple)
        action_buttons_layout.addWidget(self.collect_button) # On l'ajoute à la ligne
        
        # 2. Le bouton Annuler
        self.cancel_button = QPushButton(" ANNULER")
        self.cancel_button.setEnabled(False) # Grisé par défaut
        self.cancel_button.setStyleSheet("""
            QPushButton { background-color: #e74c3c; color: white; font-weight: bold; border-radius: 4px; padding: 6px;}
            QPushButton:hover { background-color: #c0392b; }
            QPushButton:disabled { background-color: #bdc3c7; color: #ecf0f1; }
        """)
        self.cancel_button.clicked.connect(self.annuler_collecte)
        action_buttons_layout.addWidget(self.cancel_button) # On l'ajoute à la ligne à côté du premier
        
        # 3. On ajoute la ligne complète au panneau principal
        top_layout.addLayout(action_buttons_layout)
        # =====================================================================

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        top_layout.addWidget(self.progress_bar)

        # Zone Logs
        bottom_widget = QWidget()
        bottom_layout = QVBoxLayout(bottom_widget)
        bottom_layout.setContentsMargins(0, 10, 0, 0)
        logs_title = QLabel("Logs"); logs_title.setObjectName("titleLabel")
        self.log_text_edit = QTextEdit(); self.log_text_edit.setReadOnly(True)
        bottom_layout.addWidget(logs_title)
        bottom_layout.addWidget(self.log_text_edit)

        self.splitter.addWidget(top_widget)
        self.splitter.addWidget(bottom_widget)
        self.splitter.setSizes([600, 200]) # 500 pixels pour les sources, 300 pour les logs
        left_vbox.addWidget(self.splitter)

        self.selected_polygon_geometry = None

        # --- PANNEAU DROIT (CARTE + OVERLAY) ---
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        
        self.map_container = QWidget()
        self.map_container_layout = QVBoxLayout(self.map_container)
        self.map_container_layout.setContentsMargins(0, 0, 0, 0)

        if FOLIUM_AVAILABLE:
            self.map_view = QWebEngineView()
            self.map_container_layout.addWidget(self.map_view)
            
            # On initialise le manager
            self.map_manager = MapManager(self.map_view, self.log_message)
            self.map_manager.setup_map(BASE_DIR)

            # On connecte le signal de dessin de BBOX à ta méthode locale existante
            self.map_manager.handler.bbox_drawn.connect(self.on_bbox_drawn_on_map)
            
            # On charge la logique JS quand la page est prête
            js_path = os.path.join(BASE_DIR, 'assets', 'map_logic.js')
            self.map_view.loadFinished.connect(lambda: self.map_manager.load_js_logic(js_path))
            
            # Installation de l'Overlay
            self.search_overlay = OverlaySearchWidget(self.map_container)
            self.search_overlay.raise_() # S'assure qu'il est au-dessus de la carte
            self.search_overlay.move(150, 15)
            self.search_overlay.show()
            
            # Connexion des nouveaux signaux
            self.search_overlay.type_select.currentTextChanged.connect(self._on_admin_type_changed)
            self.search_overlay.territory_select.activated.connect(self._on_territory_selected)
            self.search_overlay.territory_select.lineEdit().returnPressed.connect(
                lambda: self._on_territory_selected(self.search_overlay.territory_select.currentIndex())
            )
        else:
            self.map_container_layout.addWidget(QLabel("Folium non disponible."))

        right_layout.addWidget(self.map_container)
        main_layout.addWidget(left_container, 1)
        main_layout.addWidget(right_panel, 3)

        self.populate_data_sources_list()
        self.territoires_data = {
            "Commune": self._load_geojson_assets("communes_simplifie.geojson"),
            "EPCI": self._load_geojson_assets("epci_simplifie.geojson")
        }

        # Chargement des contours précis 
        self.territoires_data_hd = {}
        for key, path in [("Communes", "assets/communes.geojson"), ("EPCI", "assets/epci.geojson")]:
            try:
                if os.path.exists(path):
                    with open(path, "r", encoding="utf-8") as f:
                        self.territoires_data_hd[key] = json.load(f)
                else:
                    logger.warning(f"Fichier HD introuvable : {path}")
            except Exception as e:
                logger.error(f"Erreur chargement HD {key} : {e}")

        self.search_overlay.btn_group.buttonClicked.connect(self._update_map_on_clip_change)
        self.current_territory_code = None

        # --- CONNEXION AU SYSTEME DE LOG CENTRAL ---
        log_emitter.log_signal.connect(self.afficher_log_colore)

        # --- LANCEMENT DE LA VÉRIFICATION DES SOURCES EN ARRIÈRE-PLAN ---
        logger.info("Vérification de l'état des sources de données en cours...")
        self.validator_thread = SourceValidatorWorker(self.loaded_data_sources)
        self.validator_thread.validation_result_signal.connect(self.on_source_validated)
        self.validator_thread.finished_signal.connect(self.on_validation_finished)
        self.validator_thread.start()
        self.tout_decocher()

    def annuler_collecte(self):
        """Déclenchée quand l'utilisateur clique sur le bouton Annuler."""
        # 1. On vide la liste d'attente (pour ne pas lancer les sources suivantes)
        if hasattr(self, 'collection_queue'):
            self.collection_queue.clear()

        # 2. On TUE le processus en cours instantanément
        if hasattr(self, 'collector_thread') and self.collector_thread and self.collector_thread.isRunning():
            logger.warning("ARRÊT IMMÉDIAT EN COURS...")
            
            # La méthode forte : coupe l'alimentation du thread
            self.collector_thread.terminate()
            self.collector_thread.wait() # On attend un quart de seconde pour être sûr qu'il est bien mort
            
            # 3. Comme le thread a été tué brutalement, il n'enverra pas son signal "Terminé"
            # On doit donc remettre l'interface au propre nous-mêmes manuellement :
            logger.warning("=== COLLECTE ANNULÉE ===")
            self.progress_bar.setVisible(False)
            self.cancel_button.setEnabled(False)
            self.cancel_button.setText(" ANNULER")
            self.set_buttons_enabled(True) # On réactive le bouton "Collecter"

    def _update_map_on_clip_change(self):
        logger.debug(f"Index barre de recherche: {self.search_overlay.territory_select.currentIndex()}")
        if self.search_overlay.territory_select.currentIndex() <= 0:
            return

        # Si on a déjà une géométrie HD sélectionnée, on l'utilise
        if self.selected_polygon_geometry:
            geojson_dict = geometry.mapping(self.selected_polygon_geometry)
            is_precise = self.search_overlay.btn_precise.isChecked()

            # On utilise le manager
            self.map_manager.run_js_draw(geojson_dict, is_precise, False)

    def populate_data_sources_list(self):
        self.sources_list_widget.clear()

        # --- 1. Définition de l'ordre d'affichage ---
        ordre_categories = [
            "DESCRIPTION DU TERRITOIRE",
            "OFFRE DE TRANSPORT",
            "PRATIQUE DE DÉPLACEMENT",
            "AUTRES" # Sécurité : pour ranger les sources qui n'auraient pas de catégorie
        ]

        # --- 2. Tri des sources par catégorie ---
        sources_groupees = {cat: [] for cat in ordre_categories}
        
        for source in self.loaded_data_sources:
            # On va chercher la catégorie dans la config de la source
            cat = source.config.get("categorie", "AUTRES")
            if cat not in sources_groupees:
                sources_groupees[cat] = []
            sources_groupees[cat].append(source)

        # --- 3. Remplissage de l'interface graphique ---
        for cat in ordre_categories:
            sources_de_cette_categorie = sources_groupees.get(cat, [])
            
            # S'il n'y a aucune source dans cette catégorie, on ne met pas le titre
            if not sources_de_cette_categorie:
                continue

            # A. --- CRÉATION DU TITRE DE CATÉGORIE ---
            item_titre = QListWidgetItem()
            # LA CLÉ EST ICI : Il est "Enabled" (donc pas de voile gris) mais on ne peut pas le sélectionner !
            item_titre.setFlags(Qt.ItemFlag.ItemIsEnabled) 
            
            label_titre = QLabel(cat)
            label_titre.setStyleSheet("""
                color: #8bc34a; 
                font-weight: bold; 
                font-size: 16px; 
                background-color: transparent;
            """)
            
            # Marges : Espace en haut et en bas
            label_titre.setContentsMargins(0, 5, 0, 5)
            # ON IMPOSE LA HAUTEUR : 45 pixels, impossible que la liste l'écrase
            label_titre.setFixedHeight(35) 
            
            self.sources_list_widget.addItem(item_titre)
            item_titre.setSizeHint(label_titre.sizeHint())
            self.sources_list_widget.setItemWidget(item_titre, label_titre)

            # B. --- AJOUT DES SOURCES EN DESSOUS DU TITRE ---
            # On trie les sources selon le numéro défini dans config.py
            sources_de_cette_categorie.sort(key=lambda s: s.config.get("ordre", 99))
            
            for source in sources_de_cette_categorie:
                item_source = QListWidgetItem()
                
                # Ton widget exact avec ses paramètres
                widget_source = SourceListItemWidget(source, self.open_source_configuration)
                
                self.sources_list_widget.addItem(item_source)
                item_source.setSizeHint(widget_source.sizeHint())
                self.sources_list_widget.setItemWidget(item_source, widget_source)

    def tout_cocher(self):
        self._modifier_etat_toutes_sources(True)

    def tout_decocher(self):
        self._modifier_etat_toutes_sources(False)

    def _modifier_etat_toutes_sources(self, etat: bool):
        """Parcourt la liste et coche/décoche uniquement les widgets qui ont une case à cocher."""
        for i in range(self.sources_list_widget.count()):
            item = self.sources_list_widget.item(i)
            widget = self.sources_list_widget.itemWidget(item)
            
            # On vérifie que c'est bien une source (et pas un titre de catégorie comme "PRATIQUE DE DÉPLACEMENT")
            if widget and hasattr(widget, 'checkbox'):
                widget.checkbox.setChecked(etat)

    def open_source_configuration(self, source: SourceDeDonneesBase):
        if not source or not (params_ui := source.get_parametres_specifiques_ui()): return
        dialog_type = params_ui.get("type")
        current_options = self.current_source_config_options.get(source.nom_source)
        dialog = None
        if dialog_type == "layer_selection": dialog = LayerSelectionDialog(params_ui, current_options, self)
        elif dialog_type == "checkbox_options": dialog = GenericOptionsDialog(params_ui, current_options, self)
        else: logger.error(f"Type de dialogue non reconnu: '{dialog_type}'"); return
        if dialog.exec():
            self.current_source_config_options[source.nom_source] = dialog.get_selection()
            logger.debug(f"Configuration pour '{source.nom_source}' mise à jour.")

    def lancer_collecte_multiple(self):
        if not self.perimeter_is_defined:
            logger.warning("Veuillez d'abord définir un périmètre.")
            return
        
        # On regarde quel mode est actif
        is_precise = self.search_overlay.btn_precise.isChecked()
        
        # Interrogation WFS IGN ou utilisation du Rectangle
        if self.search_overlay.territory_select.currentIndex() > 0 and self.current_territory_code:

            if is_precise:
                # --- MODE PRÉCIS ---
                tipo = self.search_overlay.type_select.currentText()
                logger.info(f"Récupération de la géométrie haute précision pour {tipo} {self.current_territory_code}...")
                
                geom_precise = recuperer_geometrie_precise_ign(tipo, self.current_territory_code)
                
                if geom_precise:
                    # NOUVEAU : On stocke dans une variable dédiée à la collecte !
                    self.polygon_for_collection = geom_precise 
                    logger.info("Géométrie précise récupérée et appliquée au filtre.")
                else:
                    logger.warning("Échec récupération précise. Utilisation du contour simplifié.")
                    # NOUVEAU : Copie de secours si l'IGN plante
                    self.polygon_for_collection = self.selected_polygon_geometry 
            
            else:
                # --- MODE RECTANGLE ---
                logger.info("Mode Rectangle activé : utilisation de l'emprise rectangulaire personnalisée (IGN ignoré).")
                # On met le polygone de collecte à None, MAIS on ne touche plus à selected_polygon_geometry !
                self.polygon_for_collection = None


        # --- La suite de la méthode ne change pas ---
        if not self.export_directory:
            logger.warning("Veuillez choisir un dossier d'exportation."); return
            
        
        if not (perimetre := self.get_perimeter_from_ui()): return
        
        # --- PARCOURS SÉCURISÉ DES SOURCES ---
        self.collection_queue = []
        for i in range(self.sources_list_widget.count()):
            item = self.sources_list_widget.item(i)
            widget = self.sources_list_widget.itemWidget(item)
            
            # LA CORRECTION EST ICI : 
            # On vérifie si le widget existe ET si c'est bien une ligne de source (pas un titre)
            if widget and isinstance(widget, SourceListItemWidget):
                if widget.checkbox.isChecked():
                    self.collection_queue.append(widget.source)
            
        if not self.collection_queue: 
            # Un message beaucoup plus clair et incitatif pour l'utilisateur
            logger.warning("Action impossible : Veuillez cocher au moins une source de données à collecter.") 
            return
        
        logger.info(f"=== DÉBUT DE LA COLLECTE ({len(self.collection_queue)} source(s)) ===")
        self.set_buttons_enabled(False)
        self.cancel_button.setEnabled(True)
        self._start_next_collection()

    def _start_next_collection(self):
        if not self.collection_queue:
            logger.info("=== COLLECTE TERMINÉE ===")
            self.set_buttons_enabled(True)
            self.progress_bar.setVisible(False)
            self.cancel_button.setEnabled(False)
            self.cancel_button.setText(" ANNULER")
            return
        source = self.collection_queue.pop(0)

        perimetre = self.get_perimeter_from_ui()
        options_brutes = self.current_source_config_options.get(source.nom_source)
        if source.get_parametres_specifiques_ui() and source.nom_source not in self.current_source_config_options:
            params_ui = source.get_parametres_specifiques_ui()
            dialog_type = params_ui.get("type")
            if dialog_type == "layer_selection": options_brutes = [name for name, info in params_ui.get("layers", {}).items() if info.get("default_selected")]
            elif dialog_type == "checkbox_options": options_brutes = [{"id": opt.get("id"), "checked": opt.get("default_checked")} for opt in params_ui.get("options", [])]
            self.current_source_config_options[source.nom_source] = options_brutes
        options_finales = source.formater_options_collecte(options_brutes)

        self.collector_thread = CollectorWorker(self, source, self.export_directory, perimetre, options_finales)
        self.collector_thread.finished.connect(self.collector_thread.deleteLater)
        self.collector_thread.finished_signal.connect(self.on_collecte_terminee)
        self.collector_thread.step_progress_signal.connect(self.update_progress_bar)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.collector_thread.start()

    def on_collecte_terminee(self, succes, message):
        if succes:
            logger.info(f"  -> [OK] {message}")
        else:
            logger.error(f"  -> [ERREUR] {message}")
        self._start_next_collection()

    def set_buttons_enabled(self, enabled):
        self.collect_button.setEnabled(enabled)
        self.export_dir_button.setEnabled(enabled)

    def select_export_directory(self):
        if directory := QFileDialog.getExistingDirectory(self, "Choisir dossier d'export"):
            self.export_directory = directory
            
            # Texte à afficher
            full_text = f"Dossier : {directory}"
            
            # QFontMetrics est déjà importé dans ton code
            metrics = QFontMetrics(self.export_dir_label.font())
            
            # Enumération
            elided_text = metrics.elidedText(full_text, Qt.TextElideMode.ElideMiddle, 500)
            
            # Mise à jour de l'affichage
            self.export_dir_label.setText(elided_text)
            self.export_dir_label.setToolTip(directory)

    def afficher_log_colore(self, message, level):
        """Reçoit le signal du logger central et l'affiche avec la bonne couleur."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Choix de la couleur selon l'importance du message
        color = "#2c3e50" # Noir/Gris sombre par défaut (INFO)
        if level >= logging.ERROR:
            color = "#e74c3c" # Rouge pour les erreurs
            message = f"<b>{message}</b>" # En gras pour bien voir
        elif level >= logging.WARNING:
            color = "#e67e22" # Orange pour les avertissements
        elif "[OK]" in message or "SUCCÈS" in message:
            color = "#27ae60" # Vert pour les réussites (petite astuce de détection)

        formatted_message = f"<span style='color:#7f8c8d;'>[{timestamp}]</span> <span style='color:{color};'>{message}</span>"
        self.log_text_edit.append(formatted_message)
        self.log_text_edit.verticalScrollBar().setValue(self.log_text_edit.verticalScrollBar().maximum())

    def log_message(self, message):
        """
        Fonction de transition : sert de 'pont' temporaire pour les anciens 
        callbacks (comme MapManager) qui n'ont pas encore été mis à jour.
        """
        logger.info(message)

    def lancer_mise_a_jour(self, source):
        """Ouvre l'explorateur pour sélectionner les fichiers et lance la mise à jour."""
        recipe = source.config.get("update_recipe")
        if not recipe:
            logger.error(f"Aucune recette de mise à jour n'est configurée pour {source.nom_source}.")
            return

        expected_files = recipe.get("expected_files", [])
        selected_files = []

        # On demande à l'utilisateur de fournir chaque fichier listé dans la recette
        for desc in expected_files:
            filepath, _ = QFileDialog.getOpenFileName(
                self, f"Mise à jour {source.nom_source} - Sélectionnez : {desc}", "", "Tous les fichiers (*.*)"
            )
            if not filepath:
                logger.info("Mise à jour annulée par l'utilisateur.")
                return # Si l'utilisateur clique sur Annuler, on arrête tout
            selected_files.append(filepath)

        # On regarde s'il y a plusieurs fichiers (fichiers_locaux) ou un seul (local_file_config)
        if "fichiers_locaux" in source.config:
            dest_path = source.config["fichiers_locaux"] # C'est un dictionnaire !
        else:
            dest_path = source.config.get("local_file_config", {}).get("path", "")

        if not dest_path:
            logger.error("Impossible de trouver le chemin de destination dans config.py.")
            return
        
        # On bloque les boutons pour éviter que l'utilisateur clique partout pendant la mise à jour
        self.set_buttons_enabled(False)

        # On lance le travail en arrière-plan !
        self.updater_thread = UpdaterWorker(source.nom_source, recipe, selected_files, dest_path)
        self.updater_thread.finished_signal.connect(self.on_update_finished)
        self.updater_thread.start()

    def ouvrir_centre_mise_a_jour(self):
        """Ouvre la fenêtre listant les sources mettables à jour."""
        # On filtre les sources pour ne garder que celles où supports_update == True
        updatable_sources = [s for s in self.loaded_data_sources if getattr(s, 'supports_update', False)]
        
        # On ouvre la pop-up en lui passant notre fonction lancer_mise_a_jour
        dialog = UpdateCenterDialog(updatable_sources, self.lancer_mise_a_jour, self)
        dialog.exec()

    def on_update_finished(self, success, message):
      if success:
          logger.info(f"SUCCÈS : {message}")
      else:
          logger.error(f"ÉCHEC : {message}")
          logger.warning("-> Si le format du fichier INSEE a changé, contactez un expert.")
      self.set_buttons_enabled(True)

    def on_source_validated(self, nom_source, success, message):
        if not hasattr(self, 'erreurs_validation'):
            self.erreurs_validation = 0

        if success:
            logger.debug(f"{nom_source} : OK")
        else:
            self.erreurs_validation += 1
            logger.error(f"[{nom_source}] : {message}")

    def on_validation_finished(self):
        erreurs = getattr(self, 'erreurs_validation', 0)
        
        if erreurs == 0:
            logger.info("Toutes les sources sont opérationnelles.")
        else:
            logger.warning(f"Vérification terminée : {erreurs} source(s) indisponible(s).")
        self.erreurs_validation = 0

    def get_perimeter_from_ui(self):
        try:
            is_precise = self.search_overlay.btn_precise.isChecked()
            poly_to_send = getattr(self, 'selected_polygon_geometry', None)

            if is_precise:
                # On prend la géométrie de collecte (et si elle n'existe pas, on prend celle de l'UI par sécurité)
                poly_to_send = getattr(self, 'polygon_for_collection', getattr(self, 'selected_polygon_geometry', None))
            else:
                # En mode rectangle, on force None pour être sûr d'utiliser la BBOX
                poly_to_send = None

            # On détermine le type de sélection
            type_selection = "bbox" # On met 'bbox' par défaut pour le dessin manuel
            if poly_to_send is not None:
                # S'il y a un polygone, on lit ce qui est écrit dans le menu déroulant ("Commune" ou "EPCI")
                type_selection = self.search_overlay.type_select.currentText().lower()

            # 1. On s'assure que le polygone est en Lambert 93
            if poly_to_send:
                bounds = poly_to_send.bounds
                if max(abs(bounds[0]), abs(bounds[2])) < 180:
                    project = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True).transform
                    poly_to_send = transform(project, poly_to_send)
                
                # 2. CRUCIAL : On met à jour les valeurs de BBOX avec les mètres
                new_bounds = poly_to_send.bounds
                val_min_x, val_min_y, val_max_x, val_max_y = new_bounds
            else:
                # Si pas de polygone (mode rectangle manuel), on prend les champs UI
                val_min_x = float(self.min_x_edit.text())
                val_min_y = float(self.min_y_edit.text())
                val_max_x = float(self.max_x_edit.text())
                val_max_y = float(self.max_y_edit.text())

            return {
                "type": type_selection,
                "value": [val_min_x, val_min_y, val_max_x, val_max_y],
                "crs": "EPSG:2154",
                "polygon": poly_to_send
            }
        except Exception as e:
            logger.error(f"Erreur périmètre: {e}")
            return None

    def on_map_fully_loaded_activate_js_drawing(self, success):
        if not success:
            logger.error("Erreur critique: La page de la carte n'a pas pu charger.")
            return

        # On définit le chemin et on demande au manager de charger le fichier
        js_path = os.path.join(BASE_DIR, 'assets', 'map_logic.js')
        self.map_manager.load_js_logic(js_path)

    def on_bbox_drawn_on_map(self, min_lng, min_lat, max_lng, max_lat):
        try:
            target_crs = self.crs_edit.text()
            if not target_crs: logger.error("Erreur: CRS non défini pour la reprojection."); return
            bounds = gpd.GeoDataFrame([{'geometry':box(min_lng, min_lat, max_lng, max_lat)}], crs="EPSG:4326").to_crs(target_crs).total_bounds
            self.min_x_edit.setText(f"{bounds[0]:.2f}"); self.min_y_edit.setText(f"{bounds[1]:.2f}"); self.max_x_edit.setText(f"{bounds[2]:.2f}"); self.max_y_edit.setText(f"{bounds[3]:.2f}")
            self.perimeter_is_defined = True
            logger.debug("Périmètre mis à jour. Vous pouvez lancer la collecte.") 
        except Exception as e: logger.error(f"Erreur reprojection BBOX: {e}")

    def _load_geojson_assets(self, filename):
        path = os.path.join(BASE_DIR, "assets", filename)
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Erreur de chargement de l'asset {filename}: {e}")
        return None

    def _on_admin_type_changed(self, text):
        combo = self.search_overlay.territory_select
        combo.clear()
        if text not in self.territoires_data or not self.territoires_data[text]:
            return
        
        noms = []
        for feature in self.territoires_data[text]['features']:
            p = feature['properties']
            n = p.get('libgeo') or p.get('nom') or p.get('nom_com') or p.get('lib_epci')
            if n: noms.append(n)

        noms_uniques = sorted(list(set(noms)))
        combo.addItems(noms_uniques)
        
        # --- NOUVEAU COMPLETER SANS ACCENT ---
        completer = CompleterIntelligent(noms_uniques, combo)
        combo.setCompleter(completer)

    def _on_territory_selected(self, index):
        # Sécurité : on vérifie si l'index est valide (différent de "Choisir...")
        if index < 0 or self.search_overlay.territory_select.currentIndex() == 0:
            self.current_territory_code = None # Reset
            return

        nom = self.search_overlay.territory_select.currentText()
        tipo = self.search_overlay.type_select.currentText()
        
        # Mapping pour faire correspondre le singulier de l'UI avec le pluriel du chargement HD
        key_map = {"Commune": "Communes", "EPCI": "EPCI"}
        hd_key = key_map.get(tipo, tipo)
        
        target_final = None

        # 1. On cherche d'abord dans le HD (avec la bonne clé)
        if hd_key in self.territoires_data_hd:
            for f in self.territoires_data_hd[hd_key]['features']:
                p = f['properties']
                # On ajoute 'NOM' en majuscule souvent utilisé dans les fichiers IGN
                if p.get('libgeo') == nom or p.get('nom') == nom or p.get('lib_epci') == nom or p.get('NOM') == nom:
                    target_final = f
                    break
        
        # 2. Backup sur le simplifié si non trouvé
        if not target_final and tipo in self.territoires_data:
            for f in self.territoires_data[tipo]['features']:
                p = f['properties']
                if p.get('libgeo') == nom or p.get('nom') == nom or p.get('lib_epci') == nom:
                    target_final = f
                    break

        if target_final:
            # On récupère le code INSEE ou SIREN pour l'utiliser plus tard avec le WFS IGN
            props = target_final['properties']
            self.current_territory_code = (
                props.get('codgeo') or      # Souvent utilisé pour les communes
                props.get('code_insee') or  # Variante
                props.get('code_siren') or  # Souvent utilisé pour les EPCI
                props.get('code') or        # Générique
                props.get('id')             # Dernier recours
            )

            self.selected_polygon_geometry = shape(target_final['geometry'])

            is_precise = self.search_overlay.btn_precise.isChecked()
            # On envoie la géométrie au manager en forçant le zoom (True)
            self.map_manager.run_js_draw(target_final['geometry'], is_precise, True)

            # Mise à jour des coordonnées

            # 1. On projette le polygone GPS (4326) vers le Lambert 93 (2154)
            project = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True).transform
            geom_2154 = transform(project, self.selected_polygon_geometry)
            
            # 2. On récupère les limites en mètres
            bounds_2154 = geom_2154.bounds

            self.min_x_edit.setText(f"{bounds_2154[0]:.6f}")
            self.min_y_edit.setText(f"{bounds_2154[1]:.6f}")
            self.max_x_edit.setText(f"{bounds_2154[2]:.6f}")
            self.max_y_edit.setText(f"{bounds_2154[3]:.6f}")
            self.perimeter_is_defined = True # Ne pas oublier d'activer le flag de périmètre

    def update_progress_bar(self, current, total):
        if total > 0:
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(current) 