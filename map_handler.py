import os
import json
import tempfile
from PyQt6.QtCore import pyqtSignal, QObject, pyqtSlot, QUrl
from PyQt6.QtWebChannel import QWebChannel
from PyQt6.QtWebEngineCore import QWebEngineSettings

try:
    import folium
    from folium import plugins
    FOLIUM_AVAILABLE = True
except ImportError:
    FOLIUM_AVAILABLE = False

class MapInteractionHandler(QObject):
    """Gère les messages envoyés par le JavaScript vers Python"""
    bbox_drawn = pyqtSignal(float, float, float, float)
    
    edition_finished = pyqtSignal()

    def __init__(self, logger_func=print, parent=None): 
        super().__init__(parent)
        self.logger = logger_func
        
    @pyqtSlot(str)
    def receive_bbox(self, bbox_json_str):
        try:
            coords = json.loads(bbox_json_str)['geometry']['coordinates'][0]
            lngs, lats = [p[0] for p in coords], [p[1] for p in coords]
            self.bbox_drawn.emit(min(lngs), min(lats), max(lngs), max(lats))
        except Exception as e: 
            self.logger(f"Erreur traitement BBOX: {e}")
        
    @pyqtSlot()
    def finish_edition_from_js(self):
        """Reçoit l'ordre du JS de terminer l'édition (clic sur la carte)"""
        self.edition_finished.emit()

class MapManager:
    """Gère l'affichage, le chargement et les dessins sur la carte"""
    def __init__(self, web_view, logger_func):
        self.view = web_view
        self.logger = logger_func
        self.handler = MapInteractionHandler(logger_func)
        self.channel = None

    def setup_map(self, base_dir):
        """Configure la vue web, le canal et charge le HTML de base"""
        if not FOLIUM_AVAILABLE:
            return False

        page = self.view.page()
        page.profile().setHttpUserAgent("Ingetec_Data_Collector")
        page.settings().setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, True)
        
        # Setup du canal de communication
        self.channel = QWebChannel(page)
        page.setWebChannel(self.channel)
        self.channel.registerObject("pyHandler", self.handler)
        
        # Création de la carte Folium
        m = folium.Map(location=[46.2, 2.2], zoom_start=6, tiles="OpenStreetMap")
        plugins.Draw(export=False, draw_options={
            'polyline':False, 'polygon':False, 'circle':False, 
            'marker':False, 'circlemarker':False, 'rectangle':True
        },
        edit_options={
                'edit': False, 'remove': False
            }
        ).add_to(m)
        
        # Sauvegarde temporaire et injection du JS de communication
        temp_map_file = os.path.join(tempfile.gettempdir(), "map_render.html")
        m.save(temp_map_file)
        
        with open(temp_map_file, 'r', encoding='utf-8') as f: 
            html = f.read()
        
        html = html.replace('<head>', '<head><script src="qrc:///qtwebchannel/qwebchannel.js"></script>', 1)
        self.view.setHtml(html, QUrl.fromLocalFile(temp_map_file))
        return True

    def run_js_draw(self, geojson_geometry, is_precise, should_zoom):
        """Appelle la fonction JS pour dessiner un territoire sur la carte"""
        geojson_str = json.dumps(geojson_geometry)
        js_call = f"if(window.drawTerritory) {{ window.drawTerritory({geojson_str}, {'true' if is_precise else 'false'}, {'true' if should_zoom else 'false'}); }}"
        self.view.page().runJavaScript(js_call)

    def effacer_carte_js(self):
        """Ordonne au Javascript d'effacer tous les dessins sur la carte."""
        self.view.page().runJavaScript("if(window.clearMap) { window.clearMap(); }")

    def load_js_logic(self, js_path):
        """Charge le fichier map_logic.js dans la page"""
        if os.path.exists(js_path):
            with open(js_path, 'r', encoding='utf-8') as f:
                self.view.page().runJavaScript(f.read())

    def toggle_edit_mode_js(self, is_editing):
        """Ordonne à Leaflet d'afficher ou masquer les poignées de redimensionnement."""
        js_call = f"if(window.toggleRectangleEdit) {{ window.toggleRectangleEdit({'true' if is_editing else 'false'}); }}"
        self.view.page().runJavaScript(js_call)