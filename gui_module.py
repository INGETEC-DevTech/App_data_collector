# gui_module.py

import os
from shapely.geometry import shape, box
from PyQt6.QtWidgets import (QPushButton, QVBoxLayout, QHBoxLayout, QWidget, QDialog, 
                             QDialogButtonBox, QCheckBox, QScrollArea, QLabel, 
                             QComboBox, QFrame, QButtonGroup)
from PyQt6.QtGui import QIcon
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class OverlaySearchWidget(QFrame):
    """Petit panneau de recherche flottant au-dessus de la carte."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("searchOverlay")
        self.setFixedWidth(280)
        self.setFixedHeight(140)
        
        # 1. On définit le style du rectangle
        self.setStyleSheet("""
            #searchOverlay {
                background-color: white;
                border: 2px solid #2c3e50;
                border-radius: 10px;
            }
            QLabel { 
                color: #2c3e50; 
                font-weight: bold; 
                font-size: 11px; 
                background: transparent;
            }
            QComboBox { margin-bottom: 5px; }
        """)
        
        # 2. IMPORTANT : Créer le layout vertical et l'assigner au widget (self)
        layout = QVBoxLayout(self) 
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(5)

        # 3. Ajouter les éléments au layout
        layout.addWidget(QLabel("RECHERCHE DE TERRITOIRE"))
        
        self.type_select = QComboBox()
        self.type_select.addItems(["-- Choisir type --", "Commune", "EPCI"])
        layout.addWidget(self.type_select)

        self.territory_select = QComboBox()
        self.territory_select.setEditable(True)
        self.territory_select.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.territory_select.setPlaceholderText("Nom de la zone...")
        layout.addWidget(self.territory_select)
        
        # 4. Forcer l'application du layout
        self.setLayout(layout)

        # Pour la découpe précise
        # Conteneur horizontal pour les deux boutons
        self.mode_layout = QHBoxLayout()
        self.mode_layout.setSpacing(10)

        # Bouton "Découpe Précise"
        self.btn_precise = QPushButton("Précis")
        self.btn_precise.setCheckable(True)
        self.btn_precise.setChecked(True) # Actif par défaut
        self.btn_precise.setFixedWidth(80)
        self.btn_precise.setFixedHeight(28)

        # Bouton "Emprise Rectangle"
        self.btn_rectangle = QPushButton("Rectangle")
        self.btn_rectangle.setCheckable(True)
        self.btn_rectangle.setFixedWidth(80)
        self.btn_rectangle.setFixedHeight(28)

        # On les rend exclusifs (comme des boutons radio)
        self.btn_group = QButtonGroup(self)
        self.btn_group.addButton(self.btn_precise)
        self.btn_group.addButton(self.btn_rectangle)
        self.btn_group.setExclusive(True)

        # Style CSS pour l'effet "Boutons côte à côte"
        common_style = """
            QPushButton {
                background-color: #34495e; /* Gris-bleu foncé de base (Inactif) */
                color: #ecf0f1;
                border: 1px solid #2c3e50;
                border-radius: 6px;
                font-size: 10px;
                font-weight: bold;
                padding: 4px;
            }
            
            /* État SURVOLÉ uniquement si le bouton n'est PAS coché */
            /* C'est ici que l'on crée l'effet foncé pour inviter au clic */
            QPushButton:hover:!checked {
                background-color: #1a252f; 
                border: 1px solid #161f27;
            }

            /* État COCHÉ (Actif) */
            QPushButton:checked {
                background-color: #3498db; /* Bleu vif Ingetec */
                color: white;
                border: 1px solid #2980b9;
            }

            /* État COCHÉ + SURVOL : On garde la même couleur que l'état coché */
            /* Cela supprime l'effet de survol quand le bouton est déjà actif */
            QPushButton:checked:hover {
                background-color: #3498db; 
                border: 1px solid #2980b9;
            }
        """
        self.btn_precise.setStyleSheet(common_style)
        self.btn_rectangle.setStyleSheet(common_style)

        self.mode_layout.addStretch()
        self.mode_layout.addWidget(self.btn_precise)
        self.mode_layout.addWidget(self.btn_rectangle)
        self.mode_layout.addStretch()
        
        layout.addLayout(self.mode_layout)

class SourceListItemWidget(QWidget):
    def __init__(self, source_instance, config_callback, parent=None):
        super().__init__(parent)
        self.source = source_instance
        self.setMinimumHeight(45)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        
        self.checkbox = QCheckBox()
        self.checkbox.setChecked(self.source.config.get("default_selected", False))
        layout.addWidget(self.checkbox)
        
        label = QLabel(self.source.nom_source); label.setWordWrap(True)
        layout.addWidget(label, 1)
        
        if self.source.get_parametres_specifiques_ui():
            self.config_button = QPushButton()
            self.config_button.setObjectName("configButton")
            self.config_button.setToolTip(f"Configurer les options pour {self.source.nom_source}")
            self.config_button.setIcon(QIcon(os.path.join(BASE_DIR, 'icons', 'settings.svg')))
            self.config_button.clicked.connect(lambda: config_callback(self.source))
            layout.addWidget(self.config_button)

    def mousePressEvent(self, event):
        if hasattr(self, 'config_button') and self.config_button.underMouse():
            super().mousePressEvent(event)
            return
        if self.checkbox.underMouse():
            super().mousePressEvent(event)
            return
        self.checkbox.setChecked(not self.checkbox.isChecked())
        event.accept()

class LayerSelectionDialog(QDialog):
    def __init__(self, config_ui: dict, previously_selected: list | None, parent=None):
        super().__init__(parent)
        self.setWindowTitle(config_ui.get("title", "Sélectionner les couches"))
        self.setGeometry(200, 200, 500, 400)
        layout = QVBoxLayout(self)

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        layout.addWidget(scroll)

        scroll_content = QWidget()
        scroll.setWidget(scroll_content)
        
        self.checkbox_layout = QVBoxLayout(scroll_content)
        self.checkboxes = {}
        self.default_states = {}  # --- AJOUT : Pour stocker les états par défaut

        layers = config_ui.get("layers", {})
        for tech_name, info in sorted(layers.items(), key=lambda i: i[1].get("display_name", "")):
            cb = QCheckBox(info.get('display_name', tech_name))
            default_state = info.get("default_selected", False)
            self.default_states[tech_name] = default_state  # --- AJOUT : On sauvegarde l'état par défaut

            if previously_selected is not None:
                cb.setChecked(tech_name in previously_selected)
            else:
                cb.setChecked(default_state)
                
            self.checkboxes[tech_name] = cb
            self.checkbox_layout.addWidget(cb)
        
        self.checkbox_layout.addStretch()

        # --- AJOUT : Création des boutons d'action ---
        action_buttons_layout = QHBoxLayout()
        
        select_all_btn = QPushButton("Tout Sélectionner")
        select_all_btn.clicked.connect(self.select_all)
        
        deselect_all_btn = QPushButton("Tout Désélectionner")
        deselect_all_btn.clicked.connect(self.deselect_all)
        
        reset_btn = QPushButton("Défaut")
        reset_btn.clicked.connect(self.reset_to_defaults)
        
        action_buttons_layout.addWidget(select_all_btn)
        action_buttons_layout.addWidget(deselect_all_btn)
        action_buttons_layout.addStretch()
        action_buttons_layout.addWidget(reset_btn)
        
        layout.addLayout(action_buttons_layout) # On ajoute ce layout à l'interface
        # --- FIN DES AJOUTS ---

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_selection(self):
        return [name for name, cb in self.checkboxes.items() if cb.isChecked()]

    # --- AJOUT : Méthodes pour les nouveaux boutons ---
    def select_all(self):
        """Coche toutes les cases."""
        for cb in self.checkboxes.values():
            cb.setChecked(True)
            
    def deselect_all(self):
        """Décoche toutes les cases."""
        for cb in self.checkboxes.values():
            cb.setChecked(False)

    def reset_to_defaults(self):
        """Rétablit la sélection par défaut définie dans config.py."""
        for name, cb in self.checkboxes.items():
            default_state = self.default_states.get(name, False)
            cb.setChecked(default_state)

class GenericOptionsDialog(QDialog):
    def __init__(self, config_ui: dict, previously_selected: list | None, parent=None):
        super().__init__(parent)
        self.setWindowTitle(config_ui.get("title", "Options")); layout = QVBoxLayout(self)
        self.option_widgets = {}
        current_vals = {item["id"]: item["checked"] for item in previously_selected or []}
        for opt_data in config_ui.get("options", []):
            opt_id, opt_label = opt_data.get("id"), opt_data.get("label")
            if not opt_id or not opt_label: continue
            cb = QCheckBox(opt_label)
            cb.setChecked(current_vals.get(opt_id, opt_data.get("default_checked", False)))
            self.option_widgets[opt_id] = cb; layout.addWidget(cb)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept); buttons.rejected.connect(self.reject); layout.addWidget(buttons)
    def get_selection(self): return [{"id": opt_id, "checked": w.isChecked()} for opt_id, w in self.option_widgets.items()]


class UpdateCenterDialog(QDialog):
    """Fenêtre Pop-up listant toutes les sources locales mettables à jour."""
    def __init__(self, updatable_sources: list, update_callback, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Centre de Mise à Jour des Données")
        self.setMinimumSize(500, 300)
        
        layout = QVBoxLayout(self)
        
        title = QLabel("Sources locales disponibles pour une mise à jour :")
        title.setStyleSheet("font-weight: bold; font-size: 14px; margin-bottom: 10px; color: #2c3e50;")
        layout.addWidget(title)
        
        # Zone de défilement au cas où il y a beaucoup de sources
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        layout.addWidget(scroll)
        
        scroll_content = QWidget()
        scroll.setWidget(scroll_content)
        list_layout = QVBoxLayout(scroll_content)
        
        if not updatable_sources:
            list_layout.addWidget(QLabel("<i>Aucune source locale n'est actuellement configurée pour être mise à jour.</i>"))
        else:
            for source in updatable_sources:
                row = QWidget()
                row_layout = QHBoxLayout(row)
                row_layout.setContentsMargins(0, 5, 0, 5)
                
                lbl = QLabel(f"<b>{source.nom_source}</b>")
                row_layout.addWidget(lbl, 1)
                
                btn = QPushButton("Mettre à jour")
                btn.setIcon(QIcon(os.path.join(BASE_DIR, 'icons', 'folder.svg')))
                btn.setStyleSheet("""
                    QPushButton { background-color: #3498db; color: white; border-radius: 4px; padding: 6px; font-weight: bold;}
                    QPushButton:hover { background-color: #2980b9; }
                """)
                
                # Quand on clique, ça ferme la pop-up ET ça lance la mécanique dans la fenêtre principale
                def handle_click(checked=False, s=source):
                    self.accept() 
                    update_callback(s)
                    
                btn.clicked.connect(handle_click)
                row_layout.addWidget(btn)
                
                list_layout.addWidget(row)
                
                # Petite ligne de séparation
                line = QFrame(); line.setFrameShape(QFrame.Shape.HLine); line.setStyleSheet("color: #ecf0f1;")
                list_layout.addWidget(line)
        
        list_layout.addStretch()
        
        close_btn = QPushButton("Fermer")
        close_btn.clicked.connect(self.reject)
        layout.addWidget(close_btn)