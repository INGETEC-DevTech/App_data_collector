import os
import requests
import geopandas as gpd
import pandas as pd
import unicodedata
from data_sources.base_source import SourceDeDonneesBase

def generer_qml_poles(chemin_qml):
    """Génère un style QGIS catégorisé pour les Pôles d'équipements (Polygones)."""
    qml_content = """<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.28.0" styleCategories="AllStyleCategories">
  <renderer-v2 type="categorizedSymbol" attr="Echelon" forceraster="0" symbollevels="0">
    <categories>
      <category render="true" symbol="0" value="Pôle Supérieur" label="Pôle Supérieur"/>
      <category render="true" symbol="1" value="Pôle Intermédiaire" label="Pôle Intermédiaire"/>
      <category render="true" symbol="2" value="Pôle de Proximité" label="Pôle de Proximité"/>
      <category render="true" symbol="3" value="Commune fragile (non-pôle)" label="Commune fragile (non-pôle)"/>
    </categories>
    <symbols>
      <symbol type="fill" name="0" alpha="0.8" clip_to_extent="1">
        <layer class="SimpleFill" locked="0" pass="0" enabled="1">
          <prop k="color" v="215,25,28,255"/> <prop k="outline_color" v="255,255,255,255"/>
          <prop k="outline_width" v="0.3"/>
        </layer>
      </symbol>
      <symbol type="fill" name="1" alpha="0.8" clip_to_extent="1">
        <layer class="SimpleFill" locked="0" pass="0" enabled="1">
          <prop k="color" v="253,174,97,255"/> <prop k="outline_color" v="255,255,255,255"/>
          <prop k="outline_width" v="0.3"/>
        </layer>
      </symbol>
      <symbol type="fill" name="2" alpha="0.8" clip_to_extent="1">
        <layer class="SimpleFill" locked="0" pass="0" enabled="1">
          <prop k="color" v="255,255,191,255"/> <prop k="outline_color" v="150,150,150,255"/>
          <prop k="outline_width" v="0.3"/>
        </layer>
      </symbol>
      <symbol type="fill" name="3" alpha="0.5" clip_to_extent="1">
        <layer class="SimpleFill" locked="0" pass="0" enabled="1">
          <prop k="color" v="220,220,220,255"/> <prop k="outline_color" v="150,150,150,255"/>
          <prop k="outline_width" v="0.3"/>
        </layer>
      </symbol>
    </symbols>
  </renderer-v2>
</qgis>"""
    with open(chemin_qml, "w", encoding="utf-8") as f:
        f.write(qml_content)

def normaliser_texte(texte):
    """Enlève les accents et met en minuscules pour faciliter les comparaisons."""
    if pd.isna(texte): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texte)) if unicodedata.category(c) != 'Mn').lower().strip()

class BpeSource(SourceDeDonneesBase):
    def __init__(self, config: dict):
        super().__init__(config)
        file_conf = self.config.get("local_file_config", {})
        self.filepath = file_conf.get("path") 

    @property
    def supports_update(self) -> bool:
        return True

    def valider_lien(self):
        if self.filepath and os.path.exists(self.filepath):
            return True, f"Base BPE trouvée : {os.path.basename(self.filepath)}"
        return False, f"Base BPE introuvable : {self.filepath}"

    def get_parametres_specifiques_ui(self):
        return None

    def formater_options_collecte(self, valeurs_ui) -> dict:
        return {}

    def collecter_donnees(self, dossier_export_local, perimetre_selection_objet, options_specifiques):
        log = options_specifiques.get("log_callback", print)
        prog = options_specifiques.get("progress_callback")

        try:
            log("--- Démarrage de l'analyse territoriale BPE ---")
            
            # --- 1. Chargement du dictionnaire des 130 gammes ---
            base_dir_projet = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            chemin_excel = os.path.join(base_dir_projet, "assets", "BPE_gammes_equipements_2024.xlsx")
            
            if not os.path.exists(chemin_excel):
                return False, f"Dictionnaire des gammes introuvable dans assets : {chemin_excel}"
            
            # Lecture du fichier Excel en ignorant les 4 premières lignes d'en-tête (spécifique INSEE)
            df_gammes = pd.read_excel(chemin_excel, skiprows=4)
            
            # Recherche souple des colonnes 'code' et 'gamme'
            col_code = next((c for c in df_gammes.columns if 'code' in c.lower()), df_gammes.columns[0])
            col_gamme = next((c for c in df_gammes.columns if 'gamme' in c.lower()), df_gammes.columns[1])
            
            df_gammes['gamme_norm'] = df_gammes[col_gamme].apply(normaliser_texte)
            dict_gammes = df_gammes.set_index(col_code)['gamme_norm'].to_dict()
            log(f" > Dictionnaire chargé : {len(dict_gammes)} équipements ciblés.")
            
            if prog: prog(10, 100)

            # --- 2. Récupération des Communes IGN (Haute Précision) ---
            log(" > Téléchargement des contours exacts des communes (IGN WFS)...")
            bbox_coords = perimetre_selection_objet["value"] 
            bbox_str = f"{bbox_coords[0]},{bbox_coords[1]},{bbox_coords[2]},{bbox_coords[3]},urn:ogc:def:crs:EPSG::2154"
            
            url_ign = "https://data.geopf.fr/wfs/ows"
            params_ign = {
                'SERVICE': 'WFS', 'VERSION': '2.0.0', 'REQUEST': 'GetFeature',
                'TYPENAMES': 'BDTOPO_V3:commune', 'BBOX': bbox_str,
                'OUTPUTFORMAT': 'application/json', 'SRSNAME': 'EPSG:2154'
            }
            
            res = requests.get(url_ign, params=params_ign, timeout=30)
            res.raise_for_status()
            gdf_communes = gpd.GeoDataFrame.from_features(res.json(), crs="EPSG:2154")

            polygon_mask = perimetre_selection_objet.get("polygon")
            if polygon_mask is not None:
                # On vérifie que le CENTRE de la commune est dans le polygone
                gdf_communes = gdf_communes[gdf_communes.geometry.centroid.intersects(polygon_mask)].copy()
            
            log(f" > {len(gdf_communes)} communes identifiées dans la zone.")
            if prog: prog(30, 100)

            # --- 3. Extraction de la BPE brute ---
            log(" > Extraction des équipements depuis la base France...")
            filter_bbox = (bbox_coords[0], bbox_coords[1], bbox_coords[2], bbox_coords[3])
            gdf_bpe = gpd.read_file(self.filepath, bbox=filter_bbox, engine="pyogrio")
            
            if polygon_mask is not None:
                gdf_bpe = gdf_bpe[gdf_bpe.geometry.intersects(polygon_mask)].copy()

            if gdf_bpe.empty:
                return True, "Aucun équipement trouvé dans la zone."
            if prog: prog(50, 100)

            # --- 4. Jointure spatiale et Filtrage ---
            log(" > Analyse spatiale et classification par pôle...")
            # On cherche la colonne qui contient le type d'équipement dans la BPE (souvent TYPEQU)
            col_typequ = next((c for c in gdf_bpe.columns if c.upper() == 'TYPEQU'), None)
            if not col_typequ:
                # Si pas trouvée, on prend la première colonne qui ressemble à un code
                col_typequ = gdf_bpe.columns[0] 

            # On associe chaque point à sa commune IGN
            gdf_bpe_joined = gpd.sjoin(gdf_bpe, gdf_communes[['code_insee', 'nom_officiel', 'geometry']], how="inner", predicate="intersects")
            
            # On ajoute la gamme (nom unique pour éviter les conflits), et on jette le reste
            nom_col_gamme = 'gamme_etude'
            gdf_bpe_joined[nom_col_gamme] = gdf_bpe_joined[col_typequ].map(dict_gammes)
            gdf_bpe_filtre = gdf_bpe_joined.dropna(subset=[nom_col_gamme]).copy()

            # On force le format texte pour éviter les erreurs d'export GDAL
            gdf_bpe_filtre[nom_col_gamme] = gdf_bpe_filtre[nom_col_gamme].astype(str)

            # --- 5. Calcul des Scores par Commune ---
            # Pour chaque commune, on liste les équipements uniques présents par gamme
            synthese_list = []
            
            for code_insee, group in gdf_bpe_filtre.groupby('code_insee'):
                nom_com = group['nom_officiel'].iloc[0]
                
                # Comptage des types UNIQUES d'équipements présents
                prox_presents = group[group[nom_col_gamme].str.contains('proximite', na=False)][col_typequ].nunique()
                int_presents = group[group[nom_col_gamme].str.contains('intermediaire', na=False)][col_typequ].nunique()
                sup_presents = group[group[nom_col_gamme].str.contains('superieur', na=False)][col_typequ].nunique()
                
                # Calculs des pourcentages
                score_prox = (prox_presents / 26) * 100
                score_int = (int_presents / 45) * 100
                score_sup = (sup_presents / 59) * 100
                
                # Arbre de décision de l'échelon
                if score_sup > 50 and score_int > 50:
                    echelon = "Pôle Supérieur"
                elif score_int > 50 and score_prox > 50:
                    echelon = "Pôle Intermédiaire"
                elif score_prox > 50:
                    echelon = "Pôle de Proximité"
                else:
                    echelon = "Commune fragile (non-pôle)"
                    
                synthese_list.append({
                    'code_insee': code_insee,
                    'Commune': nom_com,
                    'Score Proximité (%)': round(score_prox, 1),
                    'Score Intermédiaire (%)': round(score_int, 1),
                    'Score Supérieur (%)': round(score_sup, 1),
                    'Echelon': echelon
                })
                
            df_synthese = pd.DataFrame(synthese_list)
            if prog: prog(80, 100)

            # Intégration des communes "vides" (qui n'ont aucun équipement des 130)
            communes_vides = gdf_communes[~gdf_communes['code_insee'].isin(df_synthese['code_insee'])]
            for _, row in communes_vides.iterrows():
                df_synthese.loc[len(df_synthese)] = {
                    'code_insee': row['code_insee'], 'Commune': row['nom_officiel'],
                    'Score Proximité (%)': 0.0, 'Score Intermédiaire (%)': 0.0, 'Score Supérieur (%)': 0.0,
                    'Echelon': "Commune fragile (non-pôle)"
                }

            # --- 6. EXPORTS FINAUX ---
            log(" > Génération des cartes et rapports...")
            dest_folder = os.path.join(dossier_export_local, self.config.get("export_subdirectory", "EQUIPEMENTS"))
            os.makedirs(dest_folder, exist_ok=True)
            
            # Export A : Les Points Bruts (pour garder la localisation précise)
            path_points = os.path.join(dest_folder, "Equipements_BPE_Points.gpkg")
            gdf_bpe_filtre.drop(columns=['index_right'], errors='ignore').to_file(path_points, driver="GPKG", layer="points_bpe")

            # Export B : Les Polygones des Communes avec leurs scores
            gdf_poles = gdf_communes.merge(df_synthese, on='code_insee', how='left')
            path_poles = os.path.join(dest_folder, "equipements_bpe.gpkg")
            gdf_poles.to_file(path_poles, driver="GPKG", layer="Cartographie_Poles_Equipements")
            
            # Ajout du style QML dynamique pour les polygones
            generer_qml_poles(path_poles.replace(".gpkg", ".qml"))

            # Export C : Le Rapport Excel
            excel_path = os.path.join(dest_folder, "equipements_bpe.xlsx")
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                df_synthese.to_excel(writer, sheet_name="Classification BPE", index=False)
                worksheet = writer.sheets["Classification BPE"]
                (max_row, max_col) = df_synthese.shape
                worksheet.add_table(0, 0, max_row, max_col - 1, {
                    'columns': [{'header': c} for c in df_synthese.columns],
                    'style': 'Table Style Medium 2'
                })
                worksheet.set_column(0, max_col - 1, 20)

            if prog: prog(100, 100)
            return True, f"Analyse BPE terminée. {len(df_synthese)} communes classées."

        except Exception as e:
            log(f"  ERREUR BPE : {str(e)}")
            return False, f"Erreur BPE (Consolidée) : {str(e)}"