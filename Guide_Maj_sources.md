# Guide de Mise à Jour des Données : Data Collector INGETEC

Ce document explique comment maintenir à jour les données utilisées par le "**Data Collector INGETEC**"

Il se divise en trois grandes parties :
1. **Mise à jour des bases de données locales** (via l'interface de l'application).
2. **Modification des chemins et des liens d'API** (via le fichier de configuration).
3. **Mise à jour du dictionnaire des territoires** (barre de recherche de l'application).

---

## 1. Mettre à jour les données locales via l'interface

Certaines sources de données sont stockées directement sur le serveur de Bois-Guillaume d'INGETEC (chemin d'accès : `P:\BiblioTechnique\MOBILITE\_Data\`). Lorsqu'une nouvelle version de ces données est publiée (par exemple par l'INSEE), vous pouvez la mettre à jour via l'application.

### Étape 1 : Télécharger la donnée brute
Avant de commencer, vous devez récupérer les nouveaux fichiers bruts depuis leur source officielle. 
*Tous les liens de téléchargement officiels ainsi que les types de fichiers attendus sont indiqués dans le fichier `sources_liens.md`*

### Étape 2 : Ouvrir le Centre de Mise à jour
1. Lancez l'application.
2. Dans le panneau de gauche (au-dessus de la liste des sources), cliquez sur le bouton bleu **Mise à jour des données**.
3. Une fenêtre s'ouvre, listant toutes les sources locales qu'il est possible de mettre à jour.

### Étape 3 : Lancer la mise à jour
1. Dans la liste, trouvez la source que vous souhaitez actualiser et cliquez sur le bouton **"Mettre à jour"** à côté de son nom.
2. L'application va ouvrir votre explorateur de fichiers et vous demander de sélectionner le ou les nouveaux fichiers bruts que vous venez de télécharger.
3. La fenêtre précisera quel fichier et quel type de fichier est attendu dans la barre de titre de l'explorateur de fichier .
4. Une fois les fichiers sélectionnés, l'application travaille en arrière-plan, on peut suivre l'avancement dans les logs.

### Étape 3 : Lancer la mise à jour et traitement
1. Dans la liste, trouvez la source que vous souhaitez actualiser et cliquez sur le bouton **"Mettre à jour"** à côté de son nom.
2. L'application va ouvrir votre explorateur de fichiers et vous demander de sélectionner le ou les nouveaux fichiers bruts que vous venez de télécharger.
3. La fenêtre précisera quel fichier et quel type de fichier est attendu dans la barre de titre de l'explorateur de fichiers.
4. Une fois les fichiers sélectionnés, l'application travaille en arrière-plan (suivi de l'avancement dans les logs). Durant cette étape, elle traite le fichier brut de votre ordinateur, effectue un prétraitement s'il y en a un, et l'enregistre directement sur le serveur partagé (`P:\BiblioTechnique\MOBILITE\_Data\`), en écrasant l'ancienne version.
5. Dès l'apparition du message de succès dans les logs, la mise à jour est terminée et le fichier brut que vous aviez téléchargé a été supprimé.
---

## 2. Modifier la Configuration (Chemins de fichiers et liens API)

Parfois, un lien internet (API publique) change, ou vous décidez de déplacer le dossier contenant les bases de données locales pour faire de la place sur votre disque. L'application doit en être informée. Cela se fait via un fichier de réglages central.

### Où se trouve le fichier de réglages ?
Toute la configuration est regroupée dans le fichier : `core/config.py`.
Ouvrez ce fichier avec un éditeur de texte simple (comme *Notepad++* ou *VS Code*). **Attention : Ne pas l'ouvrir avec un traitement de texte comme Word.**

**Important** : Lorsque vous modifiez un chemin ou un lien, **vous ne devez modifier que le texte situé entre les guillemets `" "`**. 
*Ne supprimez pas les guillemets ni les virgules `,` situées à la fin des lignes.*

### Exemple A : Changer un lien d'API publique
Si une source gouvernementale change l'adresse internet de son service, cherchez la ligne correspondant à l'URL dans `config.py` :

*Avant :*
```python
    "wfs_config": {
        "base_url": "https://data.geopf.fr/wfs/ows",
        "version": "2.0.0",
        "typename_parcelles": "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
    },
```

*Après modification :*
```python
    "wfs_config": {
        "base_url": "https://nouveau/lien/cadastre/jesperequecavamarcher",
        "version": "2.0.0",
        "typename_parcelles": "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
    },
```

### Exemple B : Changer l'emplacement d'un fichier de données locales
Si vous avez déplacé un gros fichier de données (par exemple le Cadastre ou la base SIRENE) sur un autre disque dur ou un autre serveur partagé, modifiez la ligne `path` (chemin) correspondante :

*Avant :*
```python
    "local_file_config": {
        "path": r"P:\BiblioTechnique\MOBILITE\_Data\Base Nationale des Aménagements Cyclables\amenagements_cyclables_bnac.gpkg",
        "native_crs": "EPSG:4326"
    },
```

*Après modification :*
```python
    "local_file_config": {
        "path": r"P:\BiblioTechnique\MOBILITE\_Data\NouveauNomCeQueVousVoulez\amenagements_cyclables_bnac.gpkg",
        "native_crs": "EPSG:4326"
    },
```
### Exemple C : Évolution de la structure des données

Les fournisseurs de données publics (comme l'INSEE ou l'IGN) modifient parfois le format de leurs bases de données (changement du nom d'une colonne, refonte d'un fichier). Si cela arrive, voici les deux changements les plus courants :

- Si la source a simplement renommé le titre d'une colonne dans son fichier, vous pouvez facilement mettre à jour cette correspondance dans le fichier `core/config.py`. 
*Par exemple, dans la section `CADASTRE_SOURCE_CONFIG`, l'application s'attend à trouver une colonne nommée exactement `"DÃ©nomination - par"`. Si l'IGN décide l'année prochaine de renommer cette colonne en "Nom_Proprietaire" dans son fichier Excel/CSV, il vous suffira de modifier la ligne correspondante dans les réglages `csv_columns` :*
  *Avant :* `"denomination": "DÃ©nomination - par"`
  *Après :* `"denomination": "Nom_Proprietaire"`

- Si le format de la donnée a drastiquement changé (ex : l'INSEE décide de fournir la base SIRENE au format `.json` au lieu de `.csv`), modifier le fichier de configuration ne suffira pas, l'application remontera des erreurs de traitement. Dans ce cas, il faudra faire appel à une personne ayant des compétences en programmation (Python) pour adapter les scripts situés dans les dossiers `data_sources/` ou `preparation_donnees/` pour la source en question.


### Étape finale : Sauvegarder
Une fois vos modifications terminées, enregistrez le fichier et **redémarrez complètement l'application**

---

## 3. Mettre à jour le Dictionnaire des Territoires (Barre de recherche)

L'application utilise un fichier appelé `territoires_dico.json` (situé dans le dossier `assets/`) pour alimenter la barre de recherche des Communes et des EPCI. 
Si de nouvelles communes fusionnent au 1er janvier, ou si vous souhaitez mettre à jour la liste officielle de l'INSEE, voici la procédure :

1. Ouvrez le dossier `preparation_donnees/` de l'application.
2. Exécutez sur le script nommé `generer_dictionnaire.py`
3. Le script va interroger automatiquement les serveurs de l'IGN et de l'API Géo pour télécharger la liste la plus récente des communes et EPCI français, ainsi que leurs coordonnées GPS de centrage.
4. Le fichier `assets/territoires_dico.json` se mettra à jour tout seul.
5. Au prochain lancement de l'application, la barre de recherche contiendra les territoires à jour !