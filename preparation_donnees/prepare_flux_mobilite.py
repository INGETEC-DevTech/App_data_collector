import os
import pandas as pd
from core.logger_config import logger

def appliquer_regles_metier(fichier_brut, fichier_fusions, fichier_epci, path_sortie, colonnes_a_renommer):
    """
    Nettoie et enrichit les flux INSEE selon les règles métier Ingetec.
    Prend en compte : PLM, Fusions de communes, Suisse/Frontaliers et corrections forcées.
    """
    if not os.path.exists(fichier_brut):
        logger.warning(f"Fichier source introuvable : {fichier_brut}")
        return False

    logger.info(f"Traitement de {os.path.basename(fichier_brut)} en cours...")

    # --- 1. CHARGEMENT DES RÉFÉRENTIELS ---
    try:
        df_fusions = pd.read_csv(fichier_fusions, delimiter=';', dtype=str)
        df_epci = pd.read_csv(fichier_epci, delimiter=';', dtype=str)
    except Exception as e:
        logger.error(f"Erreur lors du chargement des référentiels (assets) : {e}")
        return False

    # Préparation des dictionnaires de correspondance
    merger_map = df_fusions.set_index('Ancien Code Officiel')['Code Courant Officiel'].to_dict()
    epci_ref = df_epci[['insee', 'nom_membre', 'siren', 'raison_sociale']].copy()
    epci_ref.rename(columns={
        'insee': 'code_commune', 
        'nom_membre': 'nom_commune', 
        'siren': 'code_epci', 
        'raison_sociale': 'nom_epci'
    }, inplace=True)
    epci_ref.drop_duplicates(subset=['code_commune'], inplace=True)
    
    # --- 2. LECTURE ET NETTOYAGE INITIAL ---
    # Lecture en latin1 comme dans votre ancien script pour éviter les erreurs d'accents Insee
    flux_df = pd.read_csv(fichier_brut, encoding='latin1', delimiter=';', dtype=str, usecols=colonnes_a_renommer.keys())
    flux_df.rename(columns=colonnes_a_renommer, inplace=True)
    flux_df['flux'] = pd.to_numeric(flux_df['flux'], errors='coerce').fillna(0)

    # --- 3. APPLICATION DES RÈGLES MÉTIER (Issues de pre_traitement_travail_etude.py) ---
    
    # A. Corrections forcées (Priorité absolue)
    corrections_forcees = {
        '31483': '31483', '31300': '31300', '22179': '22223', 
        '89387': '89387', '47157': '47157'
    }

    for col in ['code_res', 'code_trav']:
        # 1. Appliquer corrections forcées
        flux_df[col] = flux_df[col].replace(corrections_forcees)
        # 2. Regrouper les arrondissements PLM
        # flux_df[col] = flux_df[col].replace(arr_to_ville)
        # 3. Gérer les fusions de communes (Communes nouvelles)
        flux_df[col] = flux_df[col].map(merger_map).fillna(flux_df[col])

    # --- 4. AGRÉGATION ET ENRICHISSEMENT EPCI ---
    # On agrège avant les jointures pour gagner en performance
    flux_df_agg = flux_df.groupby(['code_res', 'code_trav'], as_index=False).agg({'flux': 'sum'})
    
    # Jointure pour la Résidence
    df_final = pd.merge(flux_df_agg, epci_ref, left_on='code_res', right_on='code_commune', how='left')
    df_final.rename(columns={'nom_commune': 'nom_res', 'code_epci': 'code_epci_res', 'nom_epci': 'nom_epci_res'}, inplace=True)
    
    # Jointure pour le Travail/Études
    df_final = pd.merge(df_final, epci_ref, left_on='code_trav', right_on='code_commune', how='left', suffixes=('_res', '_trav'))
    df_final.rename(columns={'nom_commune': 'nom_trav', 'code_epci': 'code_epci_trav', 'nom_epci': 'nom_epci_trav'}, inplace=True)

    # --- 5. GESTION DES FRONTALIERS (SUISSE, etc.) ET HORS EPCI ---
    map_frontaliers = {
        'SU': 'Suisse', 'AL': 'Allemagne', 'BE': 'Belgique', 
        'LU': 'Luxembourg', 'IT': 'Italie', 'ES': 'Espagne', 
        'MC': 'Monaco', 'AD': 'Andorre'
    }
    
    def assigner_label_territoire(row):
        # 1. Si la commune est en France et dans un EPCI connu
        if pd.notna(row['nom_epci_trav']):
            return row['nom_epci_trav']
        
        code = str(row['code_trav'])
        
        # 2. Correction : On cherche les codes pays (SU, BE, AL, etc.) 
        # directement au début du code (ex: SU15U)
        prefixe = code[:2]
        if prefixe in map_frontaliers:
            return f"Frontaliers {map_frontaliers[prefixe]}"
        
        # 3. Sécurité pour les codes commençant encore par 99
        if code.startswith('99'):
            pays = map_frontaliers.get(code[2:4], "Étranger")
            return f"Frontaliers {pays}"
        
        return "Hors EPCI ou Inconnu"

    df_final['nom_epci_trav'] = df_final.apply(assigner_label_territoire, axis=1)
    
    # Nettoyage final des colonnes inutiles issues des merges
    df_final.drop(columns=['code_commune_res', 'code_commune_trav'], inplace=True, errors='ignore')

    # --- 6. EXPORT PARQUET (Haute Performance) ---
    df_final.to_parquet(path_sortie, index=False, engine='pyarrow')
    logger.info(f"Fichier généré : {path_sortie}")
    return True

def executer_mise_a_jour(sources_brutes, destinations_finales):
    """
    Point d'entrée principal appelé par l'application.
    @param sources_brutes: dict {'travail': path_csv, 'etude': path_csv}
    @param destinations_finales: dict {'travail': path_parquet, 'etude': path_parquet}
    """
    assets_dir = r"P:\BiblioTechnique\MOBILITE\_Data\_Enrichissement"
    
    f_fusions = os.path.join(assets_dir, 'com_matching-code_2025.csv')
    f_epci = os.path.join(assets_dir, 'epcicom2025.csv')

    # Mapping des colonnes brutes INSEE vers nos noms standardisés
    config_colonnes = {
        "travail": {
            'CODGEO': 'code_res', 
            'DCLT': 'code_trav', 
            'NBFLUX_C20_ACTOCC15P': 'flux'
        },
        "etude": {
            'CODGEO': 'code_res', 
            'DCETU': 'code_trav', 
            'NBFLUX_C20_SCOL02P': 'flux'
        }
    }

    for mode in ["travail", "etude"]:
        if mode in sources_brutes:
            # On s'assure que la destination est bien en .parquet pour la source
            dest = destinations_finales[mode].replace('.csv', '.parquet')
            appliquer_regles_metier(
                sources_brutes[mode], 
                f_fusions, 
                f_epci, 
                dest, 
                config_colonnes[mode]
            )