import pandas as pd

# 1. Mets ici le chemin exact du fichier Parquet que tu as téléchargé
fichier_parquet = r"C:\Users\AGO\Downloads\fr-en-carte-scolaire-colleges-publics (1).parquet"

print("Lecture du fichier brut en cours...")
df_brut = pd.read_parquet(fichier_parquet)

# 2. On cherche toutes les communes qui s'appellent Bagnols ou qui contiennent ce mot
# (Bagnols-sur-Cèze, Bagnols-en-Forêt, etc.)
df_bagnols = df_brut[df_brut['libelle_commune'].str.contains('Bagnols', case=False, na=False)]

print(f"\n--- RÉSULTAT POUR BAGNOLS ---")
print(f"Nombre de lignes trouvées dans la donnée source brute : {len(df_bagnols)}")

# 3. On affiche les premières lignes pour voir si les rues (type_et_libelle) sont remplies ou vides (NaN/None)
colonnes_a_voir = ['code_insee', 'libelle_commune', 'type_et_libelle', 'code_rne']

# On affiche toutes les lignes trouvées pour cette commune
print(df_bagnols[colonnes_a_voir].to_string())