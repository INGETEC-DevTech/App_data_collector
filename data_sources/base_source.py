# data_sources/base_source.py

from abc import ABC, abstractmethod

class SourceDeDonneesBase(ABC):
    """
    Classe de base abstraite (Interface) pour toutes les sources de données.
    Chaque nouvelle source de données DOIT hériter de cette classe.
    """

    def __init__(self, config: dict):
        """
        Initialise la source de données avec son bloc de configuration.

        Args:
            config (dict): Le dictionnaire de configuration complet pour cette source,
                           venant de config.py.
        """
        self.config = config

    @property
    def nom_source(self) -> str:
        """Retourne le nom d'affichage de la source, défini dans sa configuration."""
        return self.config.get('nom_source_ui', 'Source Inconnue')

    @property
    @abstractmethod
    def supports_update(self) -> bool:
        """
        Définit si la source est de type "Fichier Local" (True) ou "En Ligne" (False).
        Cette propriété DOIT être implémentée par chaque classe fille.
        
        Returns:
            bool: True si un fichier local peut être mis à jour, False sinon.
        """
        pass

    @abstractmethod
    def valider_lien(self) -> tuple[bool, str]:
        """
        Vérifie la connectivité ou la validité de la source.
        
        Returns:
            tuple[bool, str]: Un tuple contenant un booléen de succès et un message de statut.
        """
        pass

    @abstractmethod
    def get_parametres_specifiques_ui(self) -> dict | None:
        """
        Retourne la structure de données décrivant l'interface de configuration pour cette source.
        
        Returns:
            dict | None: Un dictionnaire pour la GUI, ou None si pas de configuration.
        """
        pass

    @abstractmethod
    def formater_options_collecte(self, valeurs_ui) -> dict:
        """
        Prend les valeurs brutes de l'UI et les formate dans le dictionnaire
        attendu par `collecter_donnees`.
        
        Args:
            valeurs_ui: Les données brutes issues du dialogue de configuration de la GUI.
            
        Returns:
            dict: Un dictionnaire d'options formaté pour `collecter_donnees`.
        """
        pass

    @abstractmethod
    def collecter_donnees(self, dossier_export_local: str, perimetre_selection_objet: dict, options_specifiques: dict) -> tuple[bool, str]:
        """
        La méthode principale qui exécute la collecte des données.
        
        Args:
            dossier_export_local (str): Le chemin du dossier où sauvegarder.
            perimetre_selection_objet (dict): La BBOX ou autre périmètre.
            options_specifiques (dict): Les options formatées par `formater_options_collecte` + progress_callback
        
        Returns:
            tuple[bool, str]: Un tuple contenant un booléen de succès et un message de résumé.
        """
        pass