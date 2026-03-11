# preparation_donnees/mise_a_jour_globale.py

import prepare_bpe
import preparation_donnees.simplifier_contours_carte as simplifier_contours_carte

def lancer_mise_a_jour_complete():
    print("======================================================")
    print("LANCEMENT DE LA MISE À JOUR GLOBALE DES DONNÉES")
    print("======================================================\n")

    try:
        # 1. Mise à jour de la BPE
        print("\n--- ÉTAPE 1 : CONSOLIDATION BPE ---")
        prepare_bpe.prepare_bpe_local_to_network()
        print("Étape 1 terminée avec succès.")

        # 2. Mise à jour des GeoJSON
        print("\n--- ÉTAPE 2 : PRÉPARATION GEOJSON ---")
        simplifier_contours_carte.executer_mise_a_jour_geojson()
        print("Étape 2 terminée avec succès.")

        # Tu pourras ajouter l'étape 3 (Filosofi, etc.) ici plus tard !

        print("\n======================================================")
        print("TOUTES LES MISES À JOUR ONT ÉTÉ EFFECTUÉES AVEC SUCCÈS")
        print("======================================================")

    except ValueError as ve:
        # Ceci intercepte nos alertes de "Schema Drift"
        print(f"\nERREUR DE FORMAT INSEE :\n{ve}")
        print("-> L'application de collecte a été protégée. Tu dois d'abord mettre à jour le script concerné.")
    except Exception as e:
        # Ceci intercepte toute autre erreur inattendue
        print(f"\nERREUR CRITIQUE INATTENDUE :\n{e}")

if __name__ == "__main__":
    lancer_mise_a_jour_complete()