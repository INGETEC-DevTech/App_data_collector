# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all

# -----------------------------------------------------------------------------
# 1. Collecte forcée des dépendances géographiques et moteurs
# -----------------------------------------------------------------------------
# On récupère tout ce qui est nécessaire pour lire les données (Gpkg, Parquet, SHP)
tmp_ret_pyogrio = collect_all('pyogrio')
tmp_ret_fiona = collect_all('fiona')
tmp_ret_geopandas = collect_all('geopandas')
tmp_ret_pyarrow = collect_all('pyarrow')  # <--- AJOUT CRUCIAL POUR LE BPE

# Fusion des listes récupérées
binaries = tmp_ret_pyogrio[1] + tmp_ret_fiona[1] + tmp_ret_geopandas[1] + tmp_ret_pyarrow[1]
datas = tmp_ret_pyogrio[0] + tmp_ret_fiona[0] + tmp_ret_geopandas[0] + tmp_ret_pyarrow[0]
hiddenimports = tmp_ret_pyogrio[2] + tmp_ret_fiona[2] + tmp_ret_geopandas[2] + tmp_ret_pyarrow[2]

# -----------------------------------------------------------------------------
# 2. Ajout de vos fichiers et imports spécifiques
# -----------------------------------------------------------------------------
# Vos assets et fichiers de config
my_datas = [
    ('icons', 'icons'), 
    ('fonts', 'fonts'), 
    ('assets', 'assets'), 
    ('data_sources', 'data_sources'),
    ('preparation_donnees', 'preparation_donnees')
]
datas += my_datas

# Vos imports dynamiques et plugins cachés
my_hiddenimports = [
    'data_sources.bd_topo_source',
    'data_sources.bnac_source',
    'data_sources.bnlc_source',
    'data_sources.bpe_source',
    'data_sources.cadastre_enrichment_pm',
    'data_sources.cadastre_source',
    'data_sources.carte_scolaire_source',
    'data_sources.filosofi_source',
    'data_sources.flux_mobilite_source',
    'data_sources.sirene_source',
    
    # Dépendances système critiques (déjà présentes mais conservées)
    'fiona.schema', 'fiona.crs', 'shapely', 'shapely.geometry', 'pyarrow.vendors'
]
hiddenimports += my_hiddenimports

# -----------------------------------------------------------------------------
# 3. Configuration de l'analyse
# -----------------------------------------------------------------------------
block_cipher = None

a = Analysis(
    ['main_app.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='main_app',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False, # On garde la console pour vérifier les erreurs au début
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['icons\\map.ico'],
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='main_app',
)