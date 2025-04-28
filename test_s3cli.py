#!/usr/bin/env python3
import subprocess
import sys

# Nom du profil défini dans .config.yaml
PROFILE = "NEWBUCKET4"
# Chemin vers votre CLI
CLI = "python s3cli.py"

def run(cmd):
    print(f"\n$ {cmd}")
    try:
        subprocess.run(cmd.split(), check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ commande échouée ({e.returncode})", file=sys.stderr)
        sys.exit(e.returncode)

def main():
    # 1) Création des placeholders pour tous les prefixes
    run(f"{CLI} --profile {PROFILE} create-prefixes")
    # 2) Vérification : affichage des objets (les placeholders sont listés)
    run(f"{CLI} --profile {PROFILE} list-objects {PROFILE}")
    # 3) Génération de règles de lifecycle avec +10 ans
    run(f"{CLI} --profile {PROFILE} batch-lifecycle 10")
    # 4) Maintenance mensuelle : supprime la plus ancienne, ajoute la suivante (+10 ans)
    # Utilisation du groupe 'lifecycle' et de la sous-commande 'populate-lifecycles'
    run(f"{CLI} --profile {PROFILE} lifecycle populate-lifecycles {PROFILE} 10")
    # 5) Affichage des règles de cycle de vie
    run(f"{CLI} --profile {PROFILE} lifecycle get {PROFILE}")
    # 6) Liste des objets via la sous-commande lifecycle
    run(f"{CLI} --profile {PROFILE} lifecycle list-objects {PROFILE}")

    print("\n✅ Tous les tests se sont terminés avec succès.")

if __name__ == "__main__":
    main()
