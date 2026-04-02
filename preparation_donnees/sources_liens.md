# Répertoire des Sources de Données

## 1. Référentiels et Enrichissements Transverses
**Dossier de stockage global :** `P:\BiblioTechnique\MOBILITE\_Data\_Enrichissement`

*Ces fichiers sont partagés et utilisés par plusieurs scripts de l'application (Flux de mobilité, dictionnaire de recherche, etc.).*

### Géographie des communes (Communes France 2025)
- **Fichier :** `communes-france-2025.csv`
- **Utilité :** Contient la liste des communes avec leurs coordonnées GPS (latitude/longitude de la mairie). Utilisé pour les calculs de distances (ex: Flux de mobilité)
- **Lien source :** 
- **Mise à jour :** 02/04/2026

### Composition intercommunale (EPCI 2025)
- **Fichier :** `epcicom2025.csv`
- **Utilité :** Associe chaque commune à son EPCI. Utilisé pour agréger les données à l'échelle métropolitaine et boucher les trous des libellés manquants.
- **Lien source :** 
- **Mise à jour :** 02/04/2026

### Historique et fusions de communes
- **Fichier :** `com_matching-code_2025.csv`
- **Utilité :** Traduit les anciens codes INSEE vers la géographie 2025 (gestion des communes nouvelles et fusions de communes). 
- **Lien source :** 
- **Mise à jour :** 02/04/2026

### Dictionnaire de recherche cartographique
- **Fichier :** `territoires_dico.json`
- **Utilité :** Alimente la barre de recherche textuelle de l'interface graphique (permet à l'utilisateur de chercher une ville ou un EPCI et de centrer la carte dessus.
- **Lien source :** API Géo de l'État (https://geo.api.gouv.fr)
- **Préparation (`generer_dictionnaire.py`) :** Requête directement l'API pour récupérer la liste à jour des communes/EPCI avec leurs coordonnées (centres et BBox) et compile le tout dans le fichier JSON stocké dans le dossier `assets`
- **Mise à jour :** 02/04/2026

## 2. Répertoire des Sources de Données Thématiques

### BD TOPO (IGN)
- **Lien source :** https://geoservices.ign.fr/documentation/services/services-geoplateforme/diffusion#70070 (Guide API)
- **Préparation :** Aucune
- **Dossier de stockage :** Aucun
- **Mise à jour :** Automatique

### Base nationale des aménagements cyclables - BNAC
- **Lien source :** https://www.data.gouv.fr/datasets/amenagements-cyclables-france-metropolitaine (fichier `.parquet`)
- **Préparation (`prepare_bnac.py`) :** Convertit le fichier source en GeoPackage
- **Fichier généré :** `amenagements_cyclables_bnac.gpkg`
- **Dossier de stockage :** `P:\BiblioTechnique\MOBILITE\_Data\Base Nationale des Aménagements Cyclables`
- **Mise à jour :** 02/04/2025


### Base Nationale des Lieux de Covoiturage - BNLC
- **Lien source :** https://www.data.gouv.fr/datasets/base-nationale-des-lieux-de-covoiturage
- **Préparation :** Aucune
- **Dossier de stockage :** Aucun
- **Mise à jour :** Automatique

### Base permanente des équipements - BPE (Insee)
- **Liens sources :** 
  - Base brute : https://www.insee.fr/fr/statistiques/8217525?sommaire=8217537 (fichier `.parquet`)
  - Table de passage : https://www.insee.fr/fr/statistiques/8217525?sommaire=8217537#documentation (fichier `.csv`)
  - Gammes d'équipements : https://www.insee.fr/fr/statistiques/8217535?sommaire=8217537 (fichier `.xlsx`)
- **Préparation (`prepare_bpe.py`) :** Jointure des 3 bases et calcul des scores d'équipements par commune (proximité/intermédiaire/supérieur)
- **Fichiers générés :**
  - `BPE_France_Enrichie.gpkg` (Couche spatiale)
  - `BPE_Scores_Communes_France.csv` (score des communes)
- **Dossiers de stockage :** `P:\BiblioTechnique\MOBILITE\_Data\Base permanente des équipements (BPE)`
- **Mise à jour :** 02/04/2025

### Cadastre
- **Lien source :** https://cadastre.data.gouv.fr/datasets/cadastre-etalab
- **Préparation :** Aucune
- **Dossier de stockage :** Aucun
- **Mise à jour :** Automatique

### Dispositif Fichier localisé social et fiscal - Filosofi (Insee)
- **Lien source :** https://www.insee.fr/fr/statistiques/4176290?sommaire=4176305 (Fichier `.zip - Geopackage` des carreaux de 200m)
- **Préparation (`prepare_filosofi.py`) :** Extraction en cascade (ZIP puis 7z) et fusion des fichiers GeoPackage de la France métropolitaine et de la Martinique / Réunion
- **Fichier généré :** `carreaux_200m_france_entiere.gpkg`
- **Dossier de stockage :** `P:\BiblioTechnique\MOBILITE\_Data\Filosofi - Carroyage INSEE 2019`
- **Mise à jour :** 02/04/2025

### Flux de mobilité (Insee)
- **Liens sources :** 
  - Domicile-Travail : https://www.insee.fr/fr/statistiques/7630376 (fichier `.csv`)
  - Domicile-Études : https://www.insee.fr/fr/statistiques/7630372 (fichier `.csv`)
- **Dépendances transverses :** `epcicom2025.csv` et `com_matching-code_2025.csv` sont utilisés au moment de la récupération des données (voir section "Référentiels Transverses").
- **Préparation (`prepare_flux_mobilite.py`) :** Nettoyage, renommage et optimisation des fichiers bruts au format Parquet.
- **Fichiers générés :** 
  - `base-flux-mobilite-domicile-lieu-travail-2020.parquet`
  - `base-flux-mobilite-domicile-lieu-etude-2020.parquet`
- **Dossier de stockage :** `P:\BiblioTechnique\MOBILITE\_Data\Flux de mobilite`
- **Mise à jour :** 02/04/2025

### Sirene (Insee)
- **Lien source :** https://portail-api.insee.fr/catalog/api/2ba0e549-5587-3ef1-9082-99cd865de66f?aq=ALL (Documentation API)
- **Préparation :** Aucune
- **Dossier de stockage :** Aucun
- **Mise à jour :** Automatique

### Carte scolaire (data.gouv.fr)
- **Liens sources :** 
  - Secteurs des collèges : https://data.education.gouv.fr/explore/assets/fr-en-carte-scolaire-colleges-publics/export/ (fichier `.parquet`)
  - Annuaire des établissements : https://data.education.gouv.fr/explore/assets/fr-en-annuaire-education/export/ (fichier `.csv`)
- **Préparation (`prepare_carte_scolaire.py`) :** Jointure entre les polygones des secteurs scolaires et l'annuaire de l'Éducation Nationale pour enrichir les données (noms, adresses), et conversion en fichier .gpkg
- **Fichiers générés :**
  - `carte_scolaire_points.gpkg` (Points géographiques des collèges)
  - `dictionnaire_rues.csv` (Table de correspondance des rues)
  - `statut_communes.csv` (Synthèse par commune)
- **Dossier de stockage :** `P:\BiblioTechnique\MOBILITE\_Data\Carte Scolaire`
- **Mise à jour :** 02/04/2025


