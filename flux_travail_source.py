# data_sources/flux_mobilite_source.py

import os
import sys
import time
import math
import unicodedata
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from geopy.distance import great_circle

# Import de la classe de base
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from .base_source import SourceDeDonneesBase

class FluxTravailSource(SourceDeDonneesBase):
    def __init__(self, config: dict):
        super().__init__(config)
        self.filepath = self.config.get("local_file_config", {}).get("path", "")
        
        # Identification automatique du mode (Travail ou Etude) via le nom du fichier
        if "etude" in str(self.filepath).lower():
            self.mode = "etude"
            self.col_flux_raw = 'NBFLUX_C20_SCOL02P'
            self.labels = {
                "residents": "Nb Étudiants Résidents",
                "emplois": "Nb Places d'Étude",
                "pct_sur_place": "% Étudiants étudiant sur place"
            }
        else:
            self.mode = "travail"
            self.col_flux_raw = 'NBFLUX_C20_ACTOCC15P'
            self.labels = {
                "residents": "Nb Actifs Résidents",
                "emplois": "Nb Emplois",
                "pct_sur_place": "% Actifs travaillant sur place"
            }

    @property
    def supports_update(self) -> bool:
        return True # On autorise la mise à jour via le nouveau Centre de Mise à Jour !

    def valider_lien(self):
        if not self.filepath or not os.path.exists(self.filepath):
            return False, f"Fichier de flux INTROUVABLE : {self.filepath}"
        return True, f"Fichier de flux OK ({self.mode})"

    def get_parametres_specifiques_ui(self):
        return None

    def formater_options_collecte(self, valeurs_ui) -> dict:
        return {}

    # --- MÉTHODES GÉOMÉTRIQUES (Copie exacte de l'ancien script) ---
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

    def normalize_insee(self, code):
        code_str = str(code).strip()
        if '.' in code_str: code_str = code_str.split('.')[0]
        if not code_str.isdigit(): return code_str
        return code_str.zfill(5)

    # --- COLLECTE PRINCIPALE ---
    def collecter_donnees(self, dossier_export_local, perimetre_selection_objet, options_specifiques):
        log_callback = options_specifiques.get("log_callback", print)
        progress_callback = options_specifiques.get("progress_callback")
        t_debut = time.perf_counter()

        log_callback(f"Début de la génération des flux ({self.mode})...")
        if progress_callback: progress_callback(5, 100)

        # 1. Chargement des référentiels depuis le dossier 'assets'
        assets_dir = os.path.join(project_root, 'assets')
        path_epci = os.path.join(assets_dir, 'epcicom2025.csv')
        path_cities = os.path.join(assets_dir, 'communes-france-2025.csv')
        path_match = os.path.join(assets_dir, 'com_matching-code_2025.csv')

        for f in [path_epci, path_cities, path_match, self.filepath]:
            if not os.path.exists(f):
                return False, f"Fichier requis introuvable : {os.path.basename(f)}"

        # 2. Préparation des villes et géométries
        log_callback("  > Chargement des référentiels géographiques...")
        df_cities = pd.read_csv(path_cities, dtype=str)
        df_cities.rename(columns={'code_insee': 'insee', 'latitude_mairie': 'latitude', 'longitude_mairie': 'longitude'}, inplace=True)
        df_cities['insee'] = df_cities['insee'].apply(self.normalize_insee)
        
        # Ajout des arrondissements (Règle métier)
        plm_coords = pd.DataFrame([
            {'insee': '75056', 'latitude': 48.8566, 'longitude': 2.3522},
            {'insee': '69123', 'latitude': 45.7640, 'longitude': 4.8357},
            {'insee': '13055', 'latitude': 43.2965, 'longitude': 5.3698}
        ])
        df_cities = pd.concat([df_cities, plm_coords]).drop_duplicates(subset=['insee'], keep='last')
        df_cities['latitude'] = pd.to_numeric(df_cities['latitude'], errors='coerce')
        df_cities['longitude'] = pd.to_numeric(df_cities['longitude'], errors='coerce')
        df_cities.dropna(subset=['latitude', 'longitude'], inplace=True)

        # Géométrie des communes (en EPSG:2154 pour intersection exacte avec le masque)
        gdf_cities = gpd.GeoDataFrame(
            df_cities, 
            geometry=gpd.points_from_xy(df_cities['longitude'], df_cities['latitude']),
            crs="EPSG:4326"
        ).to_crs("EPSG:2154")

        # 3. Filtrage Spatial (Quelles communes sont dans la sélection ?)
        mask_2154 = perimetre_selection_objet.get("polygon")
        if mask_2154 is None: return False, "Périmètre de sélection invalide."

        communes_in_poly = gdf_cities[gdf_cities.geometry.intersects(mask_2154)]['insee'].tolist()
        if not communes_in_poly:
            return True, "Aucune commune trouvée dans cette zone. Export annulé."
        
        log_callback(f"  > Zone d'étude définie : {len(communes_in_poly)} commune(s) concernée(s).")
        if progress_callback: progress_callback(20, 100)

        # 4. Chargement et Prétraitement du Fichier Brut
        log_callback("  > Lecture et prétraitement du fichier Insee (Veuillez patienter)...")
        
        # On essaie d'abord le format moderne (UTF-8), et si ça rate, on passe à l'ancien (Latin-1)
        try:
            flux_df = pd.read_csv(self.filepath, encoding='utf-8-sig', delimiter=';', dtype=str)
        except UnicodeDecodeError:
            flux_df = pd.read_csv(self.filepath, encoding='latin1', delimiter=';', dtype=str)
        
        # Renommage selon le mode
        col_dest = 'DCETU' if self.mode == 'etude' else 'DCLT'
        col_dest_name = 'L_DCETU' if self.mode == 'etude' else 'L_DCLT'
        flux_df.rename(columns={'CODGEO': 'code_res', 'LIBGEO': 'nom_res', col_dest: 'code_trav', col_dest_name: 'nom_trav', self.col_flux_raw: 'flux'}, inplace=True)
        flux_df['flux'] = pd.to_numeric(flux_df['flux'], errors='coerce').fillna(0)

        # Application stricte de la "Règle Métier" originelle
        corrections_forcees = {'31483': '31483', '31300': '31300', '22179': '22223', '89387': '89387', '47157': '47157'}
        flux_df['code_res'] = flux_df['code_res'].replace(corrections_forcees)
        flux_df['code_trav'] = flux_df['code_trav'].replace(corrections_forcees)
        
        plm_map = {'75056': [f'751{i:02d}' for i in range(1, 21)], '13055': [f'132{i:02d}' for i in range(1, 17)], '69123': [f'6938{i}' for i in range(1, 10)]}
        arr_to_ville = {arr: ville for ville, arr_list in plm_map.items() for arr in arr_list}
        flux_df['code_res'] = flux_df['code_res'].replace(arr_to_ville)
        flux_df['code_trav'] = flux_df['code_trav'].replace(arr_to_ville)

        # Agrégation post-correction
        flux_df_agg = flux_df.groupby(['code_res', 'nom_res', 'code_trav', 'nom_trav'], as_index=False).agg({'flux': 'sum'})
        if progress_callback: progress_callback(50, 100)

        # 5. Filtrage des données utiles (Origine OU Destination dans la zone)
        log_callback("  > Filtrage des flux pour la zone sélectionnée...")
        flux_filtres = flux_df_agg[
            (flux_df_agg['code_res'].isin(communes_in_poly)) | 
            (flux_df_agg['code_trav'].isin(communes_in_poly))
        ].copy()

        if flux_filtres.empty:
            return True, "Aucun flux n'a été trouvé pour ces communes."

        # 6. Enrichissement avec EPCI et Distances
        df_epci_ref = pd.read_csv(path_epci, delimiter=';', dtype=str)
        df_epci_ref.rename(columns={'insee': 'code_commune', 'siren': 'code_epci', 'raison_sociale': 'nom_epci'}, inplace=True)
        df_epci_ref['pmun_2025'] = pd.to_numeric(df_epci_ref['pmun_2025'].str.replace(' ', '', regex=False), errors='coerce').fillna(0)
        
        flux_filtres = pd.merge(flux_filtres, df_epci_ref[['code_commune', 'code_epci', 'nom_epci']], left_on='code_res', right_on='code_commune', how='left').rename(columns={'code_epci': 'code_epci_res', 'nom_epci': 'nom_epci_res'})
        flux_filtres = pd.merge(flux_filtres, df_epci_ref[['code_commune', 'code_epci', 'nom_epci']], left_on='code_trav', right_on='code_commune', how='left').rename(columns={'code_epci': 'code_epci_trav', 'nom_epci': 'nom_epci_trav'})
        
        df_inter_com_geo = pd.merge(flux_filtres, df_cities.add_suffix('_res'), left_on='code_res', right_on='insee_res', how='left')
        df_inter_com_geo = pd.merge(df_inter_com_geo, df_cities.add_suffix('_trav'), left_on='code_trav', right_on='insee_trav', how='left')
        df_inter_com_geo['Distance (km)'] = df_inter_com_geo.apply(self.calculer_distance_km, axis=1, lat1_col='latitude_res', lon1_col='longitude_res', lat2_col='latitude_trav', lon2_col='longitude_trav')

        # 7. Création des DataFrames pour Excel (Respect de la consigne)
        is_epci = perimetre_selection_objet.get("type") == "epci"
        
        # Onglet 1 : Synthèse Territoriale (Valable pour tous)
        details = []
        for com_code in communes_in_poly:
            nom_com = df_cities[df_cities['insee'] == com_code]['insee'].values # Fallback
            nom_com_reel = flux_filtres[flux_filtres['code_res'] == com_code]['nom_res'].iloc[0] if not flux_filtres[flux_filtres['code_res'] == com_code].empty else com_code
            
            f_int = flux_filtres[(flux_filtres['code_res'] == com_code) & (flux_filtres['code_trav'] == com_code)]['flux'].sum()
            f_sor = flux_filtres[(flux_filtres['code_res'] == com_code) & (flux_filtres['code_trav'] != com_code)]['flux'].sum()
            f_ent = flux_filtres[(flux_filtres['code_trav'] == com_code) & (flux_filtres['code_res'] != com_code)]['flux'].sum()
            actifs = f_int + f_sor
            emplois = f_int + f_ent
            
            details.append({
                'Commune': nom_com_reel, 'Code Commune': com_code, 
                self.labels['residents']: actifs, self.labels['emplois']: emplois, 
                'Flux Internes': f_int, 'Flux Sortants': f_sor, 'Flux Entrants': f_ent,
                self.labels['pct_sur_place']: (f_int / actifs) if actifs > 0 else 0
            })
        df_synth = pd.DataFrame(details).sort_values(self.labels['residents'], ascending=False)

        # Onglet 2 : Flux Communes-Communes (Valable pour tous)
        def determine_nature(r):
            if r['code_res'] in communes_in_poly and r['code_trav'] in communes_in_poly: return 'Interne'
            if r['code_res'] in communes_in_poly: return 'Sortant'
            return 'Entrant'

        df_inter_com_geo['Nature du Flux'] = df_inter_com_geo.apply(determine_nature, axis=1)
        df_inter_com_final = df_inter_com_geo[['nom_res', 'code_res', 'nom_trav', 'code_trav', 'flux', 'Distance (km)', 'Nature du Flux']].rename(
            columns={'nom_res': 'Origine', 'code_res': 'Code Origine', 'nom_trav': 'Destination', 'code_trav': 'Code Destination', 'flux': 'Volume'}
        )

        dict_excel = {"Synthèse Territoriale": df_synth}
        
        # Construction des onglets EPCI si sélection = EPCI
        if is_epci:
            code_epci = df_epci_ref[df_epci_ref['code_commune'].isin(communes_in_poly)]['code_epci'].mode()[0]
            nom_epci = df_epci_ref[df_epci_ref['code_epci'] == code_epci]['nom_epci'].iloc[0]
            
            # Flux Inter-EPCI
            flux_epci = flux_filtres.copy()
            flux_epci['code_epci_trav'] = flux_epci['code_epci_trav'].fillna('Hors EPCI')
            flux_epci['nom_epci_trav'] = flux_epci['nom_epci_trav'].fillna('Hors EPCI')
            df_inter_epci = flux_epci.groupby(['code_epci_res', 'nom_epci_res', 'code_epci_trav', 'nom_epci_trav']).agg(Volume=('flux','sum')).reset_index()
            df_inter_epci['Nature du Flux'] = df_inter_epci.apply(lambda r: 'Interne' if r['code_epci_res']==r['code_epci_trav'] else ('Sortant' if r['code_epci_res']==code_epci else 'Entrant'), axis=1)
            df_inter_epci.rename(columns={'nom_epci_res': 'Origine (EPCI)', 'code_epci_res': 'Code Origine', 'nom_epci_trav': 'Destination (EPCI)', 'code_epci_trav': 'Code Destination'}, inplace=True)
            
            # Flux EPCI-Communes
            sorties = flux_filtres[(flux_filtres['code_epci_res'] == code_epci) & (flux_filtres['code_epci_trav'] != code_epci)].groupby(['nom_trav', 'code_trav']).agg(Volume=('flux','sum')).reset_index()
            sorties['Origine Nom'] = nom_epci; sorties['Origine Code'] = code_epci; sorties['Nature du Flux'] = 'Sortant'
            sorties.rename(columns={'nom_trav': 'Destination Nom', 'code_trav': 'Destination Code'}, inplace=True)
            
            entrees = flux_filtres[(flux_filtres['code_epci_trav'] == code_epci) & (flux_filtres['code_epci_res'] != code_epci)].groupby(['nom_res', 'code_res']).agg(Volume=('flux','sum')).reset_index()
            entrees['Destination Nom'] = nom_epci; entrees['Destination Code'] = code_epci; entrees['Nature du Flux'] = 'Entrant'
            entrees.rename(columns={'nom_res': 'Origine Nom', 'code_res': 'Origine Code'}, inplace=True)
            
            dict_excel["Flux EPCI-EPCI"] = df_inter_epci
            dict_excel["Flux EPCI-Communes"] = pd.concat([sorties, entrees])[['Origine Nom', 'Origine Code', 'Destination Nom', 'Destination Code', 'Volume', 'Nature du Flux']]

        dict_excel["Flux Communes-Communes"] = df_inter_com_final
        if progress_callback: progress_callback(80, 100)

        # 8. Sauvegarde du fichier EXCEL formaté
        dest_folder = os.path.join(dossier_export_local, self.config.get("export_subdirectory", "FLUX"))
        os.makedirs(dest_folder, exist_ok=True)
        base_name = f"Analyse_Flux_{self.mode.capitalize()}"
        excel_path = os.path.join(dest_folder, f"{base_name}.xlsx")
        
        log_callback("  > Génération du Tableau de Bord Excel...")
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            for sheet_name, df in dict_excel.items():
                if df.empty: continue
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]
                workbook = writer.book
                fmt_int = workbook.add_format({'num_format': '#,##0'})
                fmt_pct = workbook.add_format({'num_format': '0.0%'})
                for col_num, col_name in enumerate(df.columns):
                    if '%' in col_name: worksheet.set_column(col_num, col_num, 15, fmt_pct)
                    elif 'Volume' in col_name or 'Nb ' in col_name or 'Flux' in col_name: worksheet.set_column(col_num, col_num, 15, fmt_int)
                    else: worksheet.set_column(col_num, col_num, 20)
                
                table_name = "".join(c for c in unicodedata.normalize('NFKD', sheet_name).encode('ascii', 'ignore').decode('utf-8') if c.isalnum())[:31]
                worksheet.add_table(0, 0, df.shape[0], df.shape[1] - 1, {'columns': [{'header': c} for c in df.columns], 'style': 'Table Style Medium 2', 'name': table_name, 'total_row': False})

        # 9. Sauvegarde de l'UNIQUE fichier Cartographique (GPKG) avec les belles courbes
        log_callback("  > Génération de la couche cartographique (Lignes courbes)...")
        gdf_com = pd.merge(df_inter_com_final, df_cities.add_suffix('_orig'), left_on='Code Origine', right_on='insee_orig', how='left')
        gdf_com = pd.merge(gdf_com, df_cities.add_suffix('_dest'), left_on='Code Destination', right_on='insee_dest', how='left')
        gdf_com.dropna(subset=['latitude_orig', 'longitude_orig', 'latitude_dest', 'longitude_dest'], inplace=True)
        
        # Filtre optionnel pour la carte pour ne pas la surcharger (ex: flux >= 10)
        gdf_com = gdf_com[gdf_com['Volume'] >= 5].copy() 

        # Projection en 2154 pour créer les courbes, puis retour en 4326 pour l'export standard
        geom_orig = gpd.GeoSeries.from_xy(gdf_com['longitude_orig'], gdf_com['latitude_orig'], crs="EPSG:4326").to_crs("EPSG:2154")
        geom_dest = gpd.GeoSeries.from_xy(gdf_com['longitude_dest'], gdf_com['latitude_dest'], crs="EPSG:4326").to_crs("EPSG:2154")
        
        geometries = []
        for start, end in zip(geom_orig, geom_dest):
            if start.equals(end): geometries.append(self.creer_boucle(start, radius=500))
            else: geometries.append(self.creer_courbe(start, end, curvature=0.15))
            
        gdf_final = gpd.GeoDataFrame(gdf_com, geometry=geometries, crs="EPSG:2154")
        gpkg_path = os.path.join(dest_folder, f"{base_name}_Carto.gpkg")
        gdf_final[['Origine', 'Code Origine', 'Destination', 'Code Destination', 'Volume', 'Distance (km)', 'Nature du Flux', 'geometry']].to_file(gpkg_path, layer='flux_communaux', driver="GPKG", engine="pyogrio")

        if progress_callback: progress_callback(100, 100)
        log_callback(f"Collecte terminée en {time.perf_counter() - t_debut:.2f}s.")
        return True, f"Succès : Fichier Excel et couche cartographique créés dans '{os.path.basename(dest_folder)}'."