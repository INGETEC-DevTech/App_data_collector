import os
import json
from shapely.geometry import shape, box
import tempfile
from PyQt6.QtWidgets import (QMainWindow, QPushButton, QVBoxLayout, QHBoxLayout, QWidget,
                             QTextEdit, QListWidget, QListWidgetItem,
                             QLabel, QFileDialog, QLineEdit,
                             QProgressBar, QCompleter, QSplitter)
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QUrl, QObject, pyqtSlot
from PyQt6.QtGui import QIcon, QFontMetrics
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWebEngineCore import QWebEngineSettings
from PyQt6.QtWebChannel import QWebChannel
from datetime import datetime
from shapely import geometry

try:
    import folium
    from folium import plugins
    FOLIUM_AVAILABLE = True
except ImportError:
    FOLIUM_AVAILABLE = False

from data_sources.base_source import SourceDeDonneesBase
from data_sources.bd_topo_source import BdTopoSource, recuperer_geometrie_precise_ign

from gui_module import (OverlaySearchWidget, SourceListItemWidget, 
                        LayerSelectionDialog, GenericOptionsDialog)

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

    def run(self):
        try:
            self.progress_signal.emit(f"Lancement de la collecte pour {self.data_source.nom_source}...")
            collect_options_with_log = self.options_obj.copy() if self.options_obj is not None else {}
            collect_options_with_log["log_callback"] = self.progress_signal.emit
            collect_options_with_log["progress_callback"] = self.step_progress_signal.emit
            succes, message_global = self.data_source.collecter_donnees(
                self.export_dir, self.bbox_obj, collect_options_with_log)
            self.finished_signal.emit(succes, message_global)
        except Exception as e:
            import traceback
            error_msg = f"Erreur critique durant la collecte : {e}\n{traceback.format_exc()}"
            self.progress_signal.emit(error_msg); self.finished_signal.emit(False, error_msg)

class MapInteractionHandler(QObject):
    bbox_drawn = pyqtSignal(float, float, float, float)
    def __init__(self, logger_func=print, parent=None): super().__init__(parent); self.logger = logger_func
    @pyqtSlot(str)
    def receive_bbox(self, bbox_json_str):
        try:
            coords = json.loads(bbox_json_str)['geometry']['coordinates'][0]
            lngs, lats = [p[0] for p in coords], [p[1] for p in coords]
            self.bbox_drawn.emit(min(lngs), min(lats), max(lngs), max(lats))
        except Exception as e: self.logger(f"Erreur traitement BBOX: {e}")

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
        
        sources_title = QLabel("Sources de Données"); sources_title.setObjectName("titleLabel")
        self.sources_list_widget = QListWidget()
        top_layout.addWidget(sources_title)
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

        self.collect_button = QPushButton(" LANCER LA COLLECTE")
        self.collect_button.setObjectName("launchButton")
        self.collect_button.setIcon(QIcon(os.path.join(BASE_DIR, 'icons', 'play.svg')))
        self.collect_button.clicked.connect(self.lancer_collecte_multiple)
        top_layout.addWidget(self.collect_button)
        
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

            self.setup_map()
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
                    print(f"Fichier HD introuvable : {path}")
            except Exception as e:
                print(f"Erreur chargement HD {key} : {e}")

        self.search_overlay.btn_group.buttonClicked.connect(self._update_map_on_clip_change)
        self.current_territory_code = None

    def _update_map_on_clip_change(self):
        if self.search_overlay.territory_select.currentIndex() <= 0:
            return

        # Si on a déjà une géométrie HD sélectionnée, on l'utilise
        if self.selected_polygon_geometry:
            geojson_dict = geometry.mapping(self.selected_polygon_geometry)
            geojson_str = json.dumps(geojson_dict)
            
            is_precise = self.search_overlay.btn_precise.isChecked()
            # IMPORTANT : 'false' ici pour ne pas re-zoomer lors d'un switch de bouton
            js_call = f"if(window.drawTerritory) {{ window.drawTerritory({geojson_str}, {'false' if is_precise else 'true'}, false); }}"
            if self.map_view:
                self.map_view.page().runJavaScript(js_call)

    def setup_map(self):
        """Configure la page web de la carte et le canal de communication avec Python."""
        page = self.map_view.page()
        # Autorise l'accès aux ressources distantes (OpenStreetMap)
        page.settings().setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, True)
        
        # Initialise le gestionnaire d'interaction (BBOX dessinée)
        self.map_interaction_handler = MapInteractionHandler(self.log_message)
        self.map_interaction_handler.bbox_drawn.connect(self.on_bbox_drawn_on_map)
        
        # Configure le canal de communication (QWebChannel)
        self.channel = QWebChannel(page)
        page.setWebChannel(self.channel)
        self.channel.registerObject("pyHandler", self.map_interaction_handler)
        
        # Charge le fichier HTML de Folium
        self.load_map_with_draw_and_comms()

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
            for source in sources_de_cette_categorie:
                item_source = QListWidgetItem()
                
                # Ton widget exact avec ses paramètres
                widget_source = SourceListItemWidget(source, self.open_source_configuration)
                
                self.sources_list_widget.addItem(item_source)
                item_source.setSizeHint(widget_source.sizeHint())
                self.sources_list_widget.setItemWidget(item_source, widget_source)

    def open_source_configuration(self, source: SourceDeDonneesBase):
        if not source or not (params_ui := source.get_parametres_specifiques_ui()): return
        dialog_type = params_ui.get("type")
        current_options = self.current_source_config_options.get(source.nom_source)
        dialog = None
        if dialog_type == "layer_selection": dialog = LayerSelectionDialog(params_ui, current_options, self)
        elif dialog_type == "checkbox_options": dialog = GenericOptionsDialog(params_ui, current_options, self)
        else: self.log_message(f"Type de dialogue non reconnu: '{dialog_type}'"); return
        if dialog.exec():
            self.current_source_config_options[source.nom_source] = dialog.get_selection()
            self.log_message(f"Configuration pour '{source.nom_source}' mise à jour.")

    def lancer_collecte_multiple(self):
        if not self.perimeter_is_defined:
            self.log_message("ERREUR : Veuillez d'abord définir un périmètre.")
            return
        
        # On regarde quel mode est actif
        is_precise = self.search_overlay.btn_precise.isChecked()
        
        # Interrogation WFS IGN ou utilisation du Rectangle
        if self.search_overlay.territory_select.currentIndex() > 0 and self.current_territory_code:
            
            if is_precise:
                # --- MODE PRÉCIS ---
                tipo = self.search_overlay.type_select.currentText()
                self.log_message(f"Récupération de la géométrie haute précision pour {tipo} {self.current_territory_code}...")
                
                # On utilise le log_callback pour voir les messages
                geom_precise = recuperer_geometrie_precise_ign(tipo, self.current_territory_code, log_callback=self.log_message)
                
                if geom_precise:
                    self.selected_polygon_geometry = geom_precise
                    self.log_message(f"Géométrie précise récupérée et appliquée au filtre.")
                else:
                    self.log_message(f"Échec récupération précise. Utilisation du contour simplifié.")
            
            else:
                # --- MODE RECTANGLE ---
                self.log_message("Mode Rectangle activé : utilisation de l'emprise rectangulaire personnalisée (IGN ignoré).")
                # LIGNE CRUCIALE : On efface le polygone complexe de la mémoire
                # Cela force la méthode `get_perimeter_from_ui` à lire les valeurs de ton rectangle édité !
                self.selected_polygon_geometry = None


        # --- La suite de la méthode ne change pas ---
        if not self.export_directory:
            self.log_message("ERREUR : Veuillez choisir un dossier d'exportation."); return
        
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
            
        if not self.collection_queue: self.log_message("Aucune source sélectionnée pour la collecte."); return
        
        self.log_message(f"\n### Lancement de la collecte pour {len(self.collection_queue)} source(s) ###")
        self.set_buttons_enabled(False)
        self._start_next_collection()

    def _start_next_collection(self):
        if not self.collection_queue:
            self.log_message("\n--- Toutes les collectes sont terminées. ---\n")
            self.set_buttons_enabled(True)
            self.progress_bar.setVisible(False)
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
        self.collector_thread.progress_signal.connect(self.log_message)
        self.collector_thread.finished_signal.connect(self.on_collecte_terminee)
        self.collector_thread.step_progress_signal.connect(self.update_progress_bar)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.collector_thread.start()

    def on_collecte_terminee(self, succes, message):
        self.log_message(f"\n--- Résultat pour la source ---\n{message}\n------------------------------")
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

    def log_message(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"<span style='color:#7f8c8d;'>[{timestamp}]</span> {message}"
        self.log_text_edit.append(formatted_message)
        self.log_text_edit.verticalScrollBar().setValue(self.log_text_edit.verticalScrollBar().maximum())

    def get_perimeter_from_ui(self):
        try:
            is_precise = self.search_overlay.btn_precise.isChecked()
            poly_to_send = getattr(self, 'selected_polygon_geometry', None)

            # 1. On s'assure que le polygone est en Lambert 93
            if poly_to_send:
                bounds = poly_to_send.bounds
                if max(abs(bounds[0]), abs(bounds[2])) < 180:
                    import pyproj
                    from shapely.ops import transform
                    project = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True).transform
                    poly_to_send = transform(project, poly_to_send)
                
                # 2. CRUCIAL : On met à jour les valeurs de BBOX avec les mètres
                new_bounds = poly_to_send.bounds
                val_min_x, val_min_y, val_max_x, val_max_y = new_bounds
            else:
                # Si pas de polygone (mode rectangle manuel), on prend les champs UI
                # Attention : s'ils sont en degrés, il faudra les convertir aussi !
                val_min_x = float(self.min_x_edit.text())
                val_min_y = float(self.min_y_edit.text())
                val_max_x = float(self.max_x_edit.text())
                val_max_y = float(self.max_y_edit.text())

            return {
                "type": "bbox",
                "value": [val_min_x, val_min_y, val_max_x, val_max_y],
                "crs": "EPSG:2154",
                "polygon": poly_to_send
            }
        except Exception as e:
            self.log_message(f"Erreur périmètre: {e}")
            return None
    
    def load_map_with_draw_and_comms(self):
        if not FOLIUM_AVAILABLE: return
        m = folium.Map(location=[46.2, 2.2], zoom_start=6, tiles="OpenStreetMap")
        plugins.Draw(export=False, draw_options={'polyline':False,'polygon':False,'circle':False,'marker':False,'circlemarker':False,'rectangle':True}).add_to(m)
        temp_map_file = os.path.join(tempfile.gettempdir(), "map_render.html")
        m.save(temp_map_file)
        with open(temp_map_file, 'r', encoding='utf-8') as f: html = f.read()
        html = html.replace('<head>', '<head><script src="qrc:///qtwebchannel/qwebchannel.js"></script>', 1)
        self.map_view.loadFinished.connect(self.on_map_fully_loaded_activate_js_drawing)
        self.map_view.setHtml(html, QUrl.fromLocalFile(temp_map_file))

    def on_map_fully_loaded_activate_js_drawing(self, success):
        if not success:
            self.log_message("Erreur critique: La page de la carte n'a pas pu charger.")
            return
        
        js_code = self.get_js_code()
        self.map_view.page().runJavaScript(js_code)

    def on_bbox_drawn_on_map(self, min_lng, min_lat, max_lng, max_lat):
        try:
            import geopandas as gpd
            from shapely.geometry import box
            target_crs = self.crs_edit.text()
            if not target_crs: self.log_message("Erreur: CRS non défini pour la reprojection."); return
            bounds = gpd.GeoDataFrame([{'geometry':box(min_lng, min_lat, max_lng, max_lat)}], crs="EPSG:4326").to_crs(target_crs).total_bounds
            self.min_x_edit.setText(f"{bounds[0]:.2f}"); self.min_y_edit.setText(f"{bounds[1]:.2f}"); self.max_x_edit.setText(f"{bounds[2]:.2f}"); self.max_y_edit.setText(f"{bounds[3]:.2f}")
            self.perimeter_is_defined = True
            self.log_message("Périmètre mis à jour. Vous pouvez lancer la collecte.")
        except Exception as e: self.log_message(f"Erreur reprojection BBOX: {e}")

    def _load_geojson_assets(self, filename):
        path = os.path.join(BASE_DIR, "assets", filename)
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Erreur chargement {filename}: {e}")
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
        
        completer = QCompleter(noms_uniques)
        completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        completer.setFilterMode(Qt.MatchFlag.MatchContains)
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
            # Il me soule ce print
            # print(f"Territoire sélectionné : {nom} (Code: {self.current_territory_code})")

            self.selected_polygon_geometry = shape(target_final['geometry'])
            geojson_str = json.dumps(target_final['geometry'])
            
            is_precise = self.search_overlay.btn_precise.isChecked()
            # On force le zoom à 'true' (3ème argument)
            js_call = f"if(window.drawTerritory) {{ window.drawTerritory({geojson_str}, {'false' if is_precise else 'true'}, true); }}"
            
            if self.map_view:
                self.map_view.page().runJavaScript(js_call)

            # Mise à jour des coordonnées
            bounds = self.selected_polygon_geometry.bounds
            self.min_x_edit.setText(f"{bounds[0]:.6f}")
            self.min_y_edit.setText(f"{bounds[1]:.6f}")
            self.max_x_edit.setText(f"{bounds[2]:.6f}")
            self.max_y_edit.setText(f"{bounds[3]:.6f}")
            self.perimeter_is_defined = True # Ne pas oublier d'activer le flag de périmètre

    def update_progress_bar(self, current, total):
        if total > 0:
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(current)

    def get_js_code(self):
        # On définit le chemin vers le fichier .js
        current_dir = os.path.dirname(os.path.abspath(__file__))
        js_path = os.path.join(current_dir, 'assets', 'map_logic.js')
        
        try:
            with open(js_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"Erreur lors de la lecture du fichier JS : {e}")
            return ""
