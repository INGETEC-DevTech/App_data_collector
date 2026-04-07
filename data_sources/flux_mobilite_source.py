# data_sources/flux_mobilite_source.py

import os
import sys
import time
import math
import unicodedata
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, box
from geopy.distance import great_circle

# Import de la classe de base
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from .base_source import SourceDeDonneesBase
from core.logger_config import logger

class FluxMobiliteSource(SourceDeDonneesBase):
    def __init__(self, config: dict):
        super().__init__(config)
        self.fichiers_locaux = self.config.get("fichiers_locaux", {})

    @property
    def supports_update(self) -> bool:
        return True

    def valider_lien(self):
        manquants = [mode for mode, path in self.fichiers_locaux.items() if not os.path.exists(path)]
        if manquants:
            return False, f"Fichier(s) local/locaux introuvable(s) ({', '.join(manquants)})."
        return True, "Fichiers de flux OK (Travail & Études)"

    def get_parametres_specifiques_ui(self):
        return {
            "type": "checkbox_options",
            "title": "Choix des flux à exporter",
            "options": [
                {"id": "travail", "label": "Domicile-Travail", "default_checked": True},
                {"id": "etude", "label": "Domicile-Études", "default_checked": True}
            ]
        }

    def formater_options_collecte(self, valeurs_ui) -> dict:
        return {"options": valeurs_ui if isinstance(valeurs_ui, list) else []}

    # --- MÉTHODES GÉOMÉTRIQUES ---
    def creer_courbe(self, start_point, end_point, curvature=0.15):
        mid_x = (start_point.x + end_point.x) / 2
        mid_y = (start_point.y + end_point.y) / 2
        mid_point = Point(mid_x, mid_y)
        line_vect = (end_point.x - start_point.x, end_point.y - start_point.y)
        perp_vect = (-line_vect[1], line_vect[0])
        perp_length = math.sqrt(perp_vect[0]**2 + perp_vect[1]**2)
        if perp_length == 0: return LineString([start_point, end_point])
        perp_vect_norm = (perp_vect[0] / perp_length, perp_vect[1] / perp_length)
        offset_dist = start_point.distance(end_point) * curvature
        control_point = Point(mid_point.x + perp_vect_norm[0] * offset_dist, mid_point.y + perp_vect_norm[1] * offset_dist)
        return LineString([start_point, control_point, end_point])

    def creer_boucle(self, point, radius=500, angle_deg=270):
        points = []
        start_angle_rad = math.radians(45)
        end_angle_rad = math.radians(45 + angle_deg)
        num_segments = 20
        for i in range(num_segments + 1):
            angle = start_angle_rad + (end_angle_rad - start_angle_rad) * i / num_segments
            dx = radius * math.cos(angle)
            dy = radius * math.sin(angle)
            points.append((point.x + dx, point.y + dy))
        return LineString(points)

    def calculer_distance_km(self, row, lat1_col, lon1_col, lat2_col, lon2_col):
        if pd.isna(row[lat1_col]) or pd.isna(row[lon1_col]) or pd.isna(row[lat2_col]) or pd.isna(row[lon2_col]): return None
        return great_circle((row[lat1_col], row[lon1_col]), (row[lat2_col], row[lon2_col])).kilometers

    # --- NOUVEAU : Normalisation robuste ---
    def normalize_insee(self, code):
        if pd.isna(code): return code
        code_str = str(code).strip()
        # Supprime les décimales (ex: '69123.0' -> '69123')
        if '.' in code_str: code_str = code_str.split('.')[0]
        # Cas spécifique Corse ou codes alphanumériques
        return code_str.zfill(5)

    # --- COLLECTE PRINCIPALE ---
    def collecter_donnees(self, dossier_export_local, perimetre_selection_objet, options_specifiques):
        progress_callback = options_specifiques.get("progress_callback")
        t_debut_global = time.perf_counter()

        options_cochees = [opt.get("id") for opt in options_specifiques.get("options", []) if opt.get("checked")]
        if not options_cochees:
            return False, "Aucun type de flux n'a été coché."

        # Chargement des référentiels (une seule fois)
        assets_dir = r"P:\BiblioTechnique\MOBILITE\_Data\_Enrichissement"
        path_epci = os.path.join(assets_dir, 'epcicom2025.csv')
        path_cities = os.path.join(assets_dir, 'communes-france-2025.csv')

        logger.debug("Chargement des référentiels géographiques...")
        df_cities = pd.read_csv(path_cities, dtype=str)
        df_cities.rename(columns={'code_insee': 'insee', 'latitude_mairie': 'latitude', 'longitude_mairie': 'longitude'}, inplace=True)
        df_cities['insee'] = df_cities['insee'].apply(self.normalize_insee)
        
        # Ajout manuel des centres PLM pour sécuriser les centroïdes
        plm_coords = pd.DataFrame([
            {'insee': '75056', 'latitude': 48.8566, 'longitude': 2.3522},
            {'insee': '69123', 'latitude': 45.7640, 'longitude': 4.8357},
            {'insee': '13055', 'latitude': 43.2965, 'longitude': 5.3698}
        ])
        df_cities = pd.concat([df_cities, plm_coords]).drop_duplicates(subset=['insee'], keep='last')
        df_cities['latitude'] = pd.to_numeric(df_cities['latitude'], errors='coerce')
        df_cities['longitude'] = pd.to_numeric(df_cities['longitude'], errors='coerce')
        df_cities.dropna(subset=['latitude', 'longitude'], inplace=True)

        gdf_cities = gpd.GeoDataFrame(
            df_cities, geometry=gpd.points_from_xy(df_cities['longitude'], df_cities['latitude']), crs="EPSG:4326"
        ).to_crs("EPSG:2154")

        # Filtrage Spatial
        mask_2154 = perimetre_selection_objet.get("polygon")
        if mask_2154 is None: 
            bbox_vals = perimetre_selection_objet.get("value")
            if bbox_vals and len(bbox_vals) == 4:
                mask_2154 = box(bbox_vals[0], bbox_vals[1], bbox_vals[2], bbox_vals[3])
            else:
                return False, "Périmètre de sélection invalide (Aucune coordonnée trouvée)."

        communes_in_poly = gdf_cities[gdf_cities.geometry.intersects(mask_2154)]['insee'].tolist()
        
        if not communes_in_poly:
            return True, "Aucune commune trouvée dans cette zone. Export annulé."
        
        # --- GESTION ADAPTATIVE DU PLM ---
        plm_map = {
            '75056': [f'751{i:02d}' for i in range(1, 21)], 
            '13055': [f'132{i:02d}' for i in range(1, 17)], 
            '69123': [f'6938{i}' for i in range(1, 10)]
        }
        arr_to_ville = {arr: ville for ville, arr_list in plm_map.items() for arr in arr_list}
        
        # Vérifier si l'utilisateur a sélectionné UNIQUEMENT une commune centre PLM
        is_strict_plm = len(communes_in_poly) == 1 and communes_in_poly[0] in plm_map.keys()
        
        if is_strict_plm:
            # Mode Arrondissements : On étend la liste de recherche pour inclure les arrondissements
            plm_center = communes_in_poly[0]
            communes_in_poly.extend(plm_map[plm_center])
            logger.debug(f"Mode PLM Strict activé. Conservation du détail par arrondissement pour {plm_center}.")
        else:
            logger.debug(f"Zone d'étude définie : {len(communes_in_poly)} commune(s). Agrégation du PLM activée.")

        # Chargement Référentiel EPCI
        df_epci_ref = pd.read_csv(path_epci, delimiter=';', dtype=str)
        df_epci_ref.rename(columns={'insee': 'code_commune', 'siren': 'code_epci', 'raison_sociale': 'nom_epci'}, inplace=True)

        type_selection = perimetre_selection_objet.get("type", "bbox")
        is_epci = type_selection == "epci"

        # BOUCLE SUR LES FLUX COCHÉS
        for idx, mode in enumerate(options_cochees):
            filepath = self.fichiers_locaux.get(mode)
            
            try:
                # Lecture Parquet (adapté selon le contexte fourni)
                flux_df = pd.read_parquet(filepath)
            except Exception as e:
                logger.error(f"Erreur de lecture Parquet: {e}")
                return False, f"Impossible de lire le fichier {mode}."

            # Normalisation robuste des codes INSEE
            flux_df['code_res'] = flux_df['code_res'].apply(self.normalize_insee)
            flux_df['code_trav'] = flux_df['code_trav'].apply(self.normalize_insee)
            flux_df['flux'] = pd.to_numeric(flux_df['flux'], errors='coerce').fillna(0)

            # Dictionnaire de forçage pour les noms PLM manquants
            plm_names = {}
            for i in range(1, 21): plm_names[f'751{i:02d}'] = f"Paris {i}e" if i>1 else "Paris 1er"
            for i in range(1, 17): plm_names[f'132{i:02d}'] = f"Marseille {i}e" if i>1 else "Marseille 1er"
            for i in range(1, 10): plm_names[f'6938{i}'] = f"Lyon {i}e" if i>1 else "Lyon 1er"

            # On force le nom de l'arrondissement si le code correspond, sinon on garde le nom existant
            flux_df['nom_res'] = flux_df['code_res'].map(plm_names).combine_first(flux_df.get('nom_res', pd.Series(dtype=str)))
            flux_df['nom_trav'] = flux_df['code_trav'].map(plm_names).combine_first(flux_df.get('nom_trav', pd.Series(dtype=str)))

            # --- Remplacement PLM si non-strict ---
            if not is_strict_plm:
                # Si zone large, on agrège les arrondissements en ville centre
                flux_df['code_res'] = flux_df['code_res'].replace(arr_to_ville)
                flux_df['code_trav'] = flux_df['code_trav'].replace(arr_to_ville)
                
                # Mise à jour des noms pour correspondre aux codes centres
                nom_centres = {'75056': 'Paris', '69123': 'Lyon', '13055': 'Marseille'}
                flux_df.loc[flux_df['code_res'].isin(nom_centres.keys()), 'nom_res'] = flux_df['code_res'].map(nom_centres)
                flux_df.loc[flux_df['code_trav'].isin(nom_centres.keys()), 'nom_trav'] = flux_df['code_trav'].map(nom_centres)

            # Agrégation des flux après potentiel remplacement PLM
            flux_df_agg = flux_df.groupby(
                ['code_res', 'nom_res', 'code_trav', 'nom_trav', 'code_epci_res', 'nom_epci_res', 'code_epci_trav', 'nom_epci_trav'], 
                as_index=False, dropna=False
            ).agg({'flux': 'sum'})

            # Filtrage selon les communes interceptées
            flux_filtres = flux_df_agg[
                (flux_df_agg['code_res'].isin(communes_in_poly)) | 
                (flux_df_agg['code_trav'].isin(communes_in_poly))
            ].copy()

            if flux_filtres.empty:
                logger.info(f"Aucun flux trouvé pour {mode}.")
                continue

            # --- Gestion des NaN sur les Noms (Étranger, etc.) ---
            flux_filtres['nom_res'] = flux_filtres['nom_res'].fillna(flux_filtres['nom_epci_res']).fillna("Étranger/Inconnu")
            flux_filtres['nom_trav'] = flux_filtres['nom_trav'].fillna(flux_filtres['nom_epci_trav']).fillna("Étranger/Inconnu")

            # --- Jointure SÉCURISÉE avec df_cities pour les GPS ---
            # On ne garde QUE les coordonnées du CSV pour ne pas écraser les noms du Parquet
            df_coords_only = df_cities[['insee', 'latitude', 'longitude']]
            
            df_inter_com_geo = pd.merge(flux_filtres, df_coords_only.add_suffix('_res'), left_on='code_res', right_on='insee_res', how='left')
            df_inter_com_geo = pd.merge(df_inter_com_geo, df_coords_only.add_suffix('_trav'), left_on='code_trav', right_on='insee_trav', how='left')
            
            df_inter_com_geo['Distance (km)'] = df_inter_com_geo.apply(
                self.calculer_distance_km, axis=1, 
                lat1_col='latitude_res', lon1_col='longitude_res', 
                lat2_col='latitude_trav', lon2_col='longitude_trav'
            )

            # --- CRÉATION DE LA SYNTHÈSE ---
            labels = {
                "residents": "Nb Actifs Résidents" if mode == "travail" else "Nb Étudiants Résidents", 
                "emplois": "Nb Emplois" if mode == "travail" else "Nb Places d'Étude", 
                "pct_sur_place": "% Actifs sur place" if mode == "travail" else "% Étudiants sur place"
            }
            
            details = []
            for com_code in communes_in_poly:
                f_int = flux_filtres[(flux_filtres['code_res'] == com_code) & (flux_filtres['code_trav'] == com_code)]['flux'].sum()
                f_sor = flux_filtres[(flux_filtres['code_res'] == com_code) & (flux_filtres['code_trav'] != com_code)]['flux'].sum()
                f_ent = flux_filtres[(flux_filtres['code_trav'] == com_code) & (flux_filtres['code_res'] != com_code)]['flux'].sum()
                
                if f_int + f_sor + f_ent == 0: continue
                
                nom_com_reel = flux_filtres[flux_filtres['code_res'] == com_code]['nom_res'].iloc[0] if not flux_filtres[flux_filtres['code_res'] == com_code].empty else com_code
                actifs, emplois = f_int + f_sor, f_int + f_ent
                
                details.append({
                    'Commune': nom_com_reel, 'Code Commune': com_code, 
                    labels['residents']: actifs, labels['emplois']: emplois, 
                    'Flux Internes': f_int, 'Flux Sortants': f_sor, 'Flux Entrants': f_ent,
                    labels['pct_sur_place']: (f_int / actifs) if actifs > 0 else 0
                })
            df_synth = pd.DataFrame(details).sort_values(labels['residents'], ascending=False) if details else pd.DataFrame()

            def determine_nature(r):
                if r['code_res'] in communes_in_poly and r['code_trav'] in communes_in_poly: return 'Interne'
                if r['code_res'] in communes_in_poly: return 'Sortant'
                return 'Entrant'

            df_inter_com_geo['Nature du Flux'] = df_inter_com_geo.apply(determine_nature, axis=1)
            
            # --- FLUX COMMUNES-COMMUNES ---
            df_inter_com_final = df_inter_com_geo[[
                'nom_res', 'code_res', 'nom_trav', 'code_trav', 'flux', 'Distance (km)', 'Nature du Flux'
            ]].rename(columns={
                'nom_res': 'Origine', 'code_res': 'Code Origine', 
                'nom_trav': 'Destination', 'code_trav': 'Code Destination', 
                'flux': 'Volume'
            })

            dict_excel = {"Synthèse Territoriale": df_synth, "Flux Communes-Communes": df_inter_com_final}

            # Exports Excel
            dest_folder = os.path.join(dossier_export_local, self.config.get("export_subdirectory", "FLUX_MOBILITE"))
            os.makedirs(dest_folder, exist_ok=True)
            base_name = "flux_dom-travail" if mode == "travail" else "flux_dom-etudes"
            
            with pd.ExcelWriter(os.path.join(dest_folder, f"{base_name}.xlsx"), engine='xlsxwriter') as writer:
                for sheet_name, df in dict_excel.items():
                    if df.empty: continue
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    worksheet = writer.sheets[sheet_name]
                    for col_num, col_name in enumerate(df.columns):
                        worksheet.set_column(col_num, col_num, 18)
                    table_name = "".join(c for c in unicodedata.normalize('NFKD', sheet_name).encode('ascii', 'ignore').decode('utf-8') if c.isalnum())[:31]
                    worksheet.add_table(0, 0, df.shape[0], df.shape[1] - 1, {
                        'columns': [{'header': c} for c in df.columns], 
                        'style': 'Table Style Medium 2', 
                        'name': table_name
                    })

            # --- Création du GeoPackage ---
            gdf_com = pd.merge(df_inter_com_final, df_coords_only.add_suffix('_orig'), left_on='Code Origine', right_on='insee_orig', how='left')
            gdf_com = pd.merge(gdf_com, df_coords_only.add_suffix('_dest'), left_on='Code Destination', right_on='insee_dest', how='left')
            
            # Ignorer les flux dont l'un des points est à l'étranger (manque de coordonnées) pour la carto
            gdf_com.dropna(subset=['latitude_orig', 'longitude_orig', 'latitude_dest', 'longitude_dest'], inplace=True)
            gdf_com = gdf_com[gdf_com['Volume'] >= 5].copy() 

            geom_orig = gpd.GeoSeries.from_xy(gdf_com['longitude_orig'], gdf_com['latitude_orig'], crs="EPSG:4326").to_crs("EPSG:2154")
            geom_dest = gpd.GeoSeries.from_xy(gdf_com['longitude_dest'], gdf_com['latitude_dest'], crs="EPSG:4326").to_crs("EPSG:2154")
            geometries = [self.creer_boucle(s, radius=500) if s.equals(e) else self.creer_courbe(s, e) for s, e in zip(geom_orig, geom_dest)]
                
            gdf_final = gpd.GeoDataFrame(gdf_com, geometry=geometries, crs="EPSG:2154")
            gdf_final[['Origine', 'Code Origine', 'Destination', 'Code Destination', 'Volume', 'Distance (km)', 'Nature du Flux', 'geometry']].to_file(
                os.path.join(dest_folder, f"{base_name}_Carto.gpkg"), layer='flux_communaux', driver="GPKG", engine="pyogrio"
            )
            
            if progress_callback: progress_callback(int((idx + 1) / len(options_cochees) * 100), 100)
    
        logger.debug(f"Collecte globale terminée en {time.perf_counter() - t_debut_global:.2f}s.")
        return True, "Fichiers de flux (Excel et Géométries) sauvegardés avec succès."