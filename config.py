# config.py

# ==============================================================================
# CONFIGURATION DES SOURCES DE DONNÉES
# Chaque source de données est définie par un unique dictionnaire de configuration.
# La convention de nommage est : NOM_DU_FICHIER_SOURCE_CONFIG
# ==============================================================================

# ------------------------------------------------------------------------------
# Source: BD TOPO (Service de téléchargement en ligne)
# Fichier: bd_topo_source.py -> Classe: BdTopoSource
# ------------------------------------------------------------------------------
BD_TOPO_SOURCE_CONFIG = {
    "ordre": 1,
    "default_selected": False,
    "nom_source_ui": "BD TOPO (IGN)",
    "categorie": "DESCRIPTION DU TERRITOIRE",
    "export_subdirectory": "BDTOPO",
    "wfs_config": {
        "base_url": "https://data.geopf.fr/wfs/ows",
        "version": "2.0.0"
    },
    "pagination_config": {
        "default_page_size": 5000,
        "max_retries": 2,
        "retry_delay_seconds": 5
    },
    "layers_config": {
        "BDTOPO_V3:aerodrome": {"display_name": "Aerodrome", "default_selected": True},
        "BDTOPO_V3:arrondissement": {"display_name": "Arrondissement", "default_selected": False},
        "BDTOPO_V3:arrondissement_municipal": {"display_name": "Arrondissement municipal", "default_selected": False},
        "BDTOPO_V3:bassin_versant_topographique": {"display_name": "Bassin versant topographique", "default_selected": False},
        "BDTOPO_V3:batiment": {"display_name": "Batiment", "default_selected": True},
        "BDTOPO_V3:canalisation": {"display_name": "Canalisation", "default_selected": False},
        "BDTOPO_V3:cimetiere": {"display_name": "Cimetiere", "default_selected": False},
        "BDTOPO_V3:collectivite_territoriale": {"display_name": "Collectivite territoriale", "default_selected": False},
        "BDTOPO_V3:commune": {"display_name": "Commune", "default_selected": True},
        "BDTOPO_V3:commune_associee_ou_deleguee": {"display_name": "Commune associée ou déléguée", "default_selected": False},
        "BDTOPO_V3:condominium": {"display_name": "Condominium", "default_selected": False},
        "BDTOPO_V3:construction_lineaire": {"display_name": "Construction lineaire", "default_selected": False},
        "BDTOPO_V3:construction_ponctuelle": {"display_name": "Construction ponctuelle", "default_selected": False},
        "BDTOPO_V3:construction_surfacique": {"display_name": "Construction surfacique", "default_selected": False},
        "BDTOPO_V3:cours_d_eau": {"display_name": "Cours d eau", "default_selected": True},
        "BDTOPO_V3:departement": {"display_name": "Departement", "default_selected": True},
        "BDTOPO_V3:detail_hydrographique": {"display_name": "Detail hydrographique", "default_selected": False},
        "BDTOPO_V3:detail_orographique": {"display_name": "Detail orographique", "default_selected": False},
        "BDTOPO_V3:epci": {"display_name": "Epci", "default_selected": True},
        "BDTOPO_V3:equipement_de_transport": {"display_name": "Equipement de transport", "default_selected": True},
        "BDTOPO_V3:erp": {"display_name": "Erp", "default_selected": False},
        "BDTOPO_V3:foret_publique": {"display_name": "Foret publique", "default_selected": True},
        "BDTOPO_V3:haie": {"display_name": "Haie", "default_selected": False},
        "BDTOPO_V3:itineraire_autre": {"display_name": "Itineraire autre", "default_selected": False},
        "BDTOPO_V3:lieu_dit_non_habite": {"display_name": "Lieu dit non habite", "default_selected": False},
        "BDTOPO_V3:ligne_electrique": {"display_name": "Ligne electrique", "default_selected": False},
        "BDTOPO_V3:ligne_orographique": {"display_name": "Ligne orographique", "default_selected": False},
        "BDTOPO_V3:limite_terre_mer": {"display_name": "Limite terre mer", "default_selected": True},
        "BDTOPO_V3:noeud_hydrographique": {"display_name": "Noeud hydrographique", "default_selected": False},
        "BDTOPO_V3:non_communication": {"display_name": "Non communication", "default_selected": False},
        "BDTOPO_V3:parc_ou_reserve": {"display_name": "Parc ou reserve", "default_selected": True},
        "BDTOPO_V3:piste_d_aerodrome": {"display_name": "Piste d aerodrome", "default_selected": False},
        "BDTOPO_V3:plan_d_eau": {"display_name": "Plan d eau", "default_selected": True},
        "BDTOPO_V3:point_d_acces": {"display_name": "Point d'acces", "default_selected": False},
        "BDTOPO_V3:point_de_repere": {"display_name": "Point de repere", "default_selected": False},
        "BDTOPO_V3:point_du_reseau": {"display_name": "Point du reseau", "default_selected": False},
        "BDTOPO_V3:poste_de_transformation": {"display_name": "Poste de transformation", "default_selected": False},
        "BDTOPO_V3:pylone": {"display_name": "Pylone", "default_selected": False},
        "BDTOPO_V3:region": {"display_name": "Region", "default_selected": True},
        "BDTOPO_V3:reservoir": {"display_name": "Reservoir", "default_selected": False},
        "BDTOPO_V3:route_numerotee_ou_nommee": {"display_name": "Route numerotee ou nommee", "default_selected": True},
        "BDTOPO_V3:section_de_points_de_repere": {"display_name": "Section de points de repere", "default_selected": False},
        "BDTOPO_V3:surface_hydrographique": {"display_name": "Surface hydrographique", "default_selected": True},
        "BDTOPO_V3:terrain_de_sport": {"display_name": "Terrain de sport", "default_selected": True},
        "BDTOPO_V3:toponymie": {"display_name": "Toponymie (Général)", "default_selected": True},
        "BDTOPO_V3:transport_par_cable": {"display_name": "Transport par cable", "default_selected": False},

        "BDTOPO_V3:troncon_de_route": {
            "display_name": "Troncon de route", 
            "default_selected": True,
            "post_processing": {
                "rename_columns": {"nom_collaboratif_droite": "nom_collaboratif", "nom_voie_ban_droite":"nom_voie"},
                "drop_columns": ["nom_collaboratif_gauche","fictif","position_par_rapport_au_sol","etat_de_l_objet","date_d_apparition","date_de_confirmation","sources",
                                 "identifiants_sources","methode_d_acquisition_planimetrique","precision_planimetrique","methode_d_acquisition_altimetrique","precision_altimetrique",
                                 "itineraire_vert","periode_de_fermeture","nature_de_la_restriction","restriction_de_hauteur","restriction_de_poids_total",
                                 "restriction_de_poids_par_essieu","restriction_de_largeur","restriction_de_longueur","matieres_dangereuses_interdites","borne_debut_gauche",
                                 "borne_debut_droite","borne_fin_gauche","borne_fin_droite","alias_gauche","alias_droit","date_de_mise_en_service","cpx_toponyme_voie_verte",
                                 "cpx_nature_itineraire_autre","cpx_toponyme_itineraire_autre","delestage","source_voie_ban_gauche","source_voie_ban_droite","nom_voie_ban_gauche",
                                 "lieux_dits_ban_gauche","lieux_dits_ban_droite","identifiant_voie_ban_gauche","identifiant_voie_ban_droite","aire_de_retournement_dfci","gabarit_dfci",
                                 "impasse_dfci","nature_detaillee_dfci","ouvrage_d_art_limitant_dfci","pente_maximale_dfci","piste_dfci","piste_dfci_debroussaillee","piste_dfci_fosses",
                                 "sens_de_circulation_dfci","tout_terrain_dfci","vitesse_moyenne_dfci","zone_de_croisement_dfci","categorie_dfci"]
            }
        },

        "BDTOPO_V3:troncon_de_voie_ferree": {"display_name": "Troncon de voie ferree", "default_selected": True},
        "BDTOPO_V3:troncon_hydrographique": {"display_name": "Troncon hydrographique", "default_selected": False},
        "BDTOPO_V3:voie_ferree_nommee": {"display_name": "Voie ferree nommee", "default_selected": False},
        "BDTOPO_V3:voie_nommee": {"display_name": "Voie nommee", "default_selected": False},
        "BDTOPO_V3:zone_d_activite_ou_d_interet": {"display_name": "Zone d activite ou d interet", "default_selected": True},
        "BDTOPO_V3:zone_d_estran": {"display_name": "Zone d estran", "default_selected": False},
        "BDTOPO_V3:zone_d_habitation": {"display_name": "Zone d habitation", "default_selected": True},
        "BDTOPO_V3:zone_de_vegetation": {"display_name": "Zone de vegetation", "default_selected": True},
    }
}

# ------------------------------------------------------------------------------
# Source: Cadastre (Service de téléchargement en ligne)
# Fichier: cadastre_source.py -> Classe: CadastreSource
# ------------------------------------------------------------------------------
CADASTRE_SOURCE_CONFIG = {
    "ordre": 2,
    "default_selected": False,
    "nom_source_ui": "Cadastre (IGN)",
    "categorie": "DESCRIPTION DU TERRITOIRE",
    "export_subdirectory": "FONCIER",
    "wfs_config": {
        "base_url": "https://data.geopf.fr/wfs/ows",
        "version": "2.0.0",
        "typename_parcelles": "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
    },
    "pagination_config": {
        "default_page_size": 3000,
        "max_retries": 2,
        "retry_delay_seconds": 5
    },
    "enrichment_pm_config": {
        "enabled": True,
        "csv_directory_path": r"P:\BiblioTechnique\MOBILITE\_Data\Parcelles des personnes morales 2024",
        "csv_file_prefix": "PM_24_NB_",
        "csv_file_extension": ".csv",
        "csv_encoding": "latin-1",
        "csv_columns": {
            "departement": "DÃ©partement", "code_commune": "Code Commune", "section": "Section",
            "no_plan": "NÂ° plan", "groupe_personne": "Groupe personne - par",
            "forme_juridique": "Forme juridique abrÃ©gÃ©e - par", "denomination": "DÃ©nomination - par"
        },
        "idu_target_lengths": {"departement": 2, "code_commune": 3, "section_prefix": "000", "section": 2, "no_plan": 4},
        "data_columns_from_csv": {"groupe_personne": "groupe_personne", "forme_juridique": "forme_juridique", "denomination": "denomination"},
        "output_column_names": {"groupe_personne": "pm_groupe", "forme_juridique": "pm_forme_juridique", "denomination": "pm_denomination"}
    }
}

# ------------------------------------------------------------------------------
# Source: BNAC (Fichier téléchargé depuis lien stable)
# Fichier: bnac_source.py -> Classe: BnacSource
# ------------------------------------------------------------------------------

BNAC_SOURCE_CONFIG = {
    "ordre": 1,
    "nom_source_ui": "Aménagements cyclables (BNAC - data.gouv.fr)",
    "categorie": "OFFRE DE TRANSPORT",
    "export_subdirectory": "VELO",
    "local_file_config": {
        "path": r"P:\BiblioTechnique\MOBILITE\_Data\Base Nationale des Aménagements Cyclables\amenagements_cyclables_bnac.gpkg",
        "native_crs": "EPSG:4326"
    },
    "update_recipe": {
        "type": "preprocessing",
        "expected_files": ["Fichier GeoJSON de la France entière"],
        "script_to_run": "prepare_bnac"
    }
}

# ------------------------------------------------------------------------------
# Source: Filosofi (Fichier Local)
# Fichier: filosofi_source.py -> Classe: FilosofiSource
# ------------------------------------------------------------------------------
FILOSOFI_SOURCE_CONFIG = {
    "ordre": 3,
    "default_selected": False,
    "nom_source_ui": "Données socio-économiques carroyées (FiLoSoFi - INSEE)",
    "categorie": "DESCRIPTION DU TERRITOIRE",
    "export_subdirectory": "SOCIO-ECO",
    "local_file_config": {
        "path": r"P:\BiblioTechnique\MOBILITE\_Data\Filosofi - Carroyage INSEE 2019\carreaux_200m_france_entiere.gpkg",
        "native_crs": "EPSG:4326",
        "layer_name": "carreaux_200m_france_entiere"
    },
    "update_recipe": {
        "type": "preprocessing",
        "expected_files": ["Archive ZIP Filosofi de l'Insee (.zip)"],
        "script_to_run": "prepare_filosofi"
    }
}

# ------------------------------------------------------------------------------
# Source: SIRENE (API "Live")
# Fichier: sirene_source.py -> Classe: SireneSource
# ------------------------------------------------------------------------------
SIRENE_SOURCE_CONFIG = {
    "ordre": 4,
    "default_selected": False,
    "nom_source_ui": "Entreprises (SIRENE - INSEE)",
    "categorie": "DESCRIPTION DU TERRITOIRE",
    "export_subdirectory": "SOCIO-ECO",
    "api_config": {
        "base_url": "https://api.insee.fr/api-sirene/3.11",
        "api_key": "8c20e503-37f4-49ee-b90b-5be1fa8a5c04",
        "api_header_name": "X-INSEE-Api-Key-Integration",
        "requests_per_minute_limit": 30
    }
}

# ------------------------------------------------------------------------------
# Source: BNLC (Fichier téléchargé depuis lien stable)
# Fichier: bnlc_source.py -> Classe: BnlcSource
# ------------------------------------------------------------------------------

BNLC_SOURCE_CONFIG = {
    "ordre": 2,
    "default_selected": False,
    "nom_source_ui": "Lieux de covoiturage (BNLC - data.gouv.fr)",
    "categorie": "OFFRE DE TRANSPORT",
    "export_subdirectory": "VL-PL",
    "api_config": {
        # URL directe extraite de votre fichier JSON
        "csv_url": "https://transport.data.gouv.fr/resources/81372/download",
        "native_crs": "EPSG:4326"
    }
}

# ------------------------------------------------------------------------------
# Source: Équipements (BPE - Fichier Local)
# Fichier: bpe_source.py -> Classe: BpeSource
# ------------------------------------------------------------------------------
BPE_SOURCE_CONFIG = {
    "ordre": 5,
    "default_selected": False,
    "nom_source_ui": "Equipements (BPE - INSEE)",
    "categorie": "DESCRIPTION DU TERRITOIRE",
    "export_subdirectory": "EQUIPEMENTS",
    "local_file_config": {
        "path": r"P:\BiblioTechnique\MOBILITE\_Data\Base permanente des équipements (BPE)\BPE24_France_Enrichie.gpkg",
        "native_crs": "EPSG:2154" 
    },
    "local_file_config_scores": {
        "path": r"P:\BiblioTechnique\MOBILITE\_Data\Base permanente des équipements (BPE)\BPE24_Scores_Communes_France.csv"
    },
    "update_recipe": {
        "type": "preprocessing",
        "expected_files": ["Fichier Parquet BPE (.parquet)", "Table de passage (.csv)", "Fichier Gammes (.xlsx)"],
        "script_to_run": "prepare_bpe_local_to_network"
    }
}

#
# ------------------------------------------------------------------------------
# Source: Flux Mobilité (Fichiers CSV sur réseau + Géométrie interne)
# Fichier: flux_mobilite_source.py -> Classe: FluxMobiliteSource
# ------------------------------------------------------------------------------
FLUX_MOBILITE_SOURCE_CONFIG = {
    "ordre": 2,
    "default_selected": False,
    "nom_source_ui": "Flux de Mobilité (INSEE)",
    "categorie": "PRATIQUE DE DÉPLACEMENT",
    "export_subdirectory": "COMPTAGES-FLUX",
    "fichiers_locaux": {
        "travail": r"P:\BiblioTechnique\MOBILITE\_Data\Flux Mobilite\base-flux-mobilite-domicile-lieu-travail-2020.parquet",
        "etude": r"P:\BiblioTechnique\MOBILITE\_Data\Flux Mobilite\base-flux-mobilite-domicile-lieu-etude-2020.parquet"
    },
    "update_recipe": {
        "type": "preprocessing",
        "expected_files": ["Fichier Domicile-TRAVAIL (Actifs)", "Fichier Domicile-ÉTUDES (Étudiants)"],
        "script_to_run": "prepare_flux_mobilite"
    },
}

# ------------------------------------------------------------------------------
# Source: Carte Scolaire (Fichiers locaux)
# ------------------------------------------------------------------------------
CARTE_SCOLAIRE_SOURCE_CONFIG = {
    "ordre": 2,
    "default_selected": False,
    "nom_source_ui": "Carte scolaire (data.gouv.fr)",
    "categorie": "PRATIQUE DE DÉPLACEMENT",
    "export_subdirectory": "CARTE_SCOLAIRE",
    "local_file_config": {
        "path": r"P:\BiblioTechnique\MOBILITE\_Data\Carte Scolaire\carte_scolaire_points.gpkg", 
        "path_csv_rues": r"P:\BiblioTechnique\MOBILITE\_Data\Carte Scolaire\dictionnaire_rues.csv",
        "path_csv_statuts": r"P:\BiblioTechnique\MOBILITE\_Data\Carte Scolaire\statut_communes.csv",
        "native_crs": "EPSG:4326" 
    },
    "update_recipe": {
        "type": "preprocessing",
        "expected_files": ["Fichier des secteurs scolaires (.parquet)", "Fichier de l'annuaire (.csv)"],
        "script_to_run": "prepare_carte_scolaire"
    }
}