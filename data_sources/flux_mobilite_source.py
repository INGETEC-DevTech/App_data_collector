# data_sources/flux_mobilite_source.py

import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, box
from .base_source import SourceDeDonneesBase

class FluxMobiliteSource(SourceDeDonneesBase):
    def supports_update(self) -> bool:
        return True 

    def valider_lien(self) -> tuple[bool, str]:
        """Vérifie que les fichiers CSV configurés existent bien sur le réseau."""
        sources = self.config.get("csv_sources", {})
        missing = []
        for key, info in sources.items():
            path = info.get("path")
            if not path or not os.path.exists(path):
                missing.append(f"{key} ({path})")
        
        if missing:
            return False, f"Fichiers manquants : {', '.join(missing)}"
        return True, "Fichiers sources accessibles."

    def get_parametres_specifiques_ui(self) -> dict | None:
        """Affiche des cases à cocher pour choisir quel type de flux exporter."""
        options = []
        sources = self.config.get("csv_sources", {})
        for key, info in sources.items():
            options.append({
                "id": key,
                "label": info.get("label", key),
                "default_checked": True
            })
            
        return {
            "type": "checkbox_options",
            "title": "Choix des Flux",
            "options": options
        }

    def formater_options_collecte(self, valeurs_ui) -> dict:
        """Récupère simplement quels types (travail/etudes) sont cochés."""
        selected_types = [item["id"] for item in valeurs_ui if item["checked"]]
        return {"types_flux": selected_types}

    def collecter_donnees(self, dossier_export_local: str, perimetre_selection_objet: dict, options_specifiques: dict) -> tuple[bool, str]:
        log = options_specifiques.get("log_callback", print)
        types_a_traiter = options_specifiques.get("types_flux", [])
        
        if not types_a_traiter:
            return False, "Aucun type de flux sélectionné."

        # 1. Chargement des géométries de référence (Communes)
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        geojson_name = self.config.get("ref_communes_filename", "communes_simplifie.geojson")
        path_communes = os.path.join(base_dir, "assets", geojson_name)

        if not os.path.exists(path_communes):
            return False, f"Fichier de référence communes introuvable : {path_communes}"

        log("Chargement du référentiel des communes (pour positionnement)...")
        try:
            gdf_communes = gpd.read_file(path_communes)
            
            # --- GESTION DU CRS (Projection) ---
            target_crs = "EPSG:2154"
            if gdf_communes.crs is None:
                gdf_communes.set_crs("EPSG:4326", inplace=True)
            
            if gdf_communes.crs.to_string() != target_crs:
                gdf_communes = gdf_communes.to_crs(target_crs)

            # --- Recherche de la colonne CODE INSEE ---
            possible_names = ['codgeo', 'code_geo', 'code', 'insee', 'code_insee']
            col_code = next((c for c in gdf_communes.columns if c.lower() in possible_names), None)
            
            if not col_code:
                return False, f"Impossible de trouver le code INSEE. Colonnes dispos: {list(gdf_communes.columns)}"
            
            # Calcul des centroïdes
            gdf_communes['centroid'] = gdf_communes.geometry.centroid
            ref_points = gdf_communes.set_index(col_code)['centroid'].to_dict()
            
            # --- Filtrage Spatial Strict ---
            bbox_list = perimetre_selection_objet.get("value")
            geom_selection = perimetre_selection_objet.get("polygon")
            
            if geom_selection is None and bbox_list:
                geom_selection = box(*bbox_list)
            
            # MODIFICATION ICI : On utilise .within() sur le centroïde pour ne garder que les communes "dedans"
            mask_strict = gdf_communes['centroid'].within(geom_selection)
            communes_selectionnees = gdf_communes[mask_strict][col_code].tolist()
            
            log(f"Zone d'étude : {len(communes_selectionnees)} communes strictement identifiées (via centroïde).")

        except Exception as e:
            return False, f"Erreur chargement/projection référentiel communes : {e}"

        exported_files = []

        # 2. Traitement des flux
        for type_flux in types_a_traiter:
            config_source = self.config["csv_sources"].get(type_flux)
            if not config_source: continue
            
            path_csv = config_source["path"]
            log(f"Traitement des {config_source['label']}...")
            
            try:
                cols_map = config_source["cols_mapping"]
                cols_csv = list(cols_map.keys())
                csv_params = self.config.get("csv_params", {})
                
                # Lecture CSV
                df = pd.read_csv(
                    path_csv, 
                    usecols=cols_csv, 
                    sep=csv_params.get("sep", ";"), 
                    encoding=csv_params.get("encoding", "utf-8-sig"),
                    dtype=csv_params.get("dtype", str)
                )
                
                # Renommage
                df.rename(columns=cols_map, inplace=True)
                
                # Filtrage : On garde si Origine OU Destination est dans la sélection
                mask = df['code_origine'].isin(communes_selectionnees) | df['code_destination'].isin(communes_selectionnees)
                df_filtered = df[mask].copy()
                
                if df_filtered.empty:
                    log(f"Aucun flux trouvé concernant la zone sélectionnée pour {type_flux}.")
                    continue
                
                log(f"  - {len(df_filtered)} flux conservés. Création des géométries...")
                
                # Ajout des géométries
                df_filtered['geometry_orig'] = df_filtered['code_origine'].map(ref_points)
                df_filtered['geometry_dest'] = df_filtered['code_destination'].map(ref_points)
                df_filtered.dropna(subset=['geometry_orig', 'geometry_dest'], inplace=True)
                
                if df_filtered.empty:
                    log(f"  - Attention : Flux trouvés mais impossible de les géolocaliser.")
                    continue

                # Création des lignes
                df_filtered['geometry'] = df_filtered.apply(
                    lambda row: LineString([row['geometry_orig'], row['geometry_dest']]), axis=1
                )
                
                gdf_flux = gpd.GeoDataFrame(df_filtered, geometry='geometry', crs=gdf_communes.crs)
                gdf_flux.drop(columns=['geometry_orig', 'geometry_dest'], inplace=True, errors='ignore')
                
                # Export
                filename = f"Flux_{type_flux.capitalize()}.gpkg"
                out_path = os.path.join(dossier_export_local, self.config["export_subdirectory"])
                os.makedirs(out_path, exist_ok=True)
                full_path = os.path.join(out_path, filename)
                
                gdf_flux.to_file(full_path, driver="GPKG", layer=f"flux_{type_flux}")
                exported_files.append(filename)
                log(f"  - Export réussi : {filename}")

            except Exception as e:
                log(f"Erreur lors du traitement de {type_flux} : {e}")

        if not exported_files:
            return False, "Aucun fichier n'a pu être généré (vérifiez les logs)."
            
        return True, f"Fichiers générés : {', '.join(exported_files)}"