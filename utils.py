import unicodedata
from PyQt6.QtCore import QStringListModel
from PyQt6.QtWidgets import QCompleter

def nettoyer_texte(texte):
    if not texte: return ""
    texte = str(texte).lower()
    texte = texte.replace('œ', 'oe').replace('æ', 'ae').replace('ç', 'c')
    texte = texte.replace('-', '').replace("'", '').replace(' ', '')
    texte = "".join(c for c in unicodedata.normalize('NFD', texte) if unicodedata.category(c) != 'Mn')
    return " ".join(texte.split())

class CompleterIntelligent(QCompleter):
    def __init__(self, items, parent=None):
        super().__init__(items, parent)
        self.items_originaux = items
        self.items_nettoyes = [nettoyer_texte(item) for item in items]
        
    def splitPath(self, path):
        texte_recherche = nettoyer_texte(path)
        resultats = [
            self.items_originaux[i] 
            for i, texte_propre in enumerate(self.items_nettoyes) 
            if texte_recherche in texte_propre
        ]
        self.setModel(QStringListModel(resultats))
        return [""]