#!/usr/bin/env python3
"""
Interface de configuration pour générer des datasets de différentes tailles
"""

import os
import sys
import subprocess
from pathlib import Path

def display_menu():
    """Afficher le menu de sélection des tailles."""
    
    print("🎯 CONFIGURATEUR DE DATASETS - ETL QUALITÉ DONNÉES")
    print("=" * 55)
    print()
    print("Choisissez la taille de données à générer:")
    print()
    print("1. 📊 DEMO (15k lignes, ~1MB)")
    print("   - Parfait pour démonstrations rapides")
    print("   - Population: 5k + 3k | Consommation: 4k + 3k")
    print()
    print("2. 📈 SMALL (190k lignes, ~8MB)")
    print("   - Configuration standard pour développement")
    print("   - Population: 50k + 25k | Consommation: 75k + 35k")
    print()
    print("3. 📊 MEDIUM (1.2M lignes, ~50MB)")
    print("   - Tests de performance moyens")
    print("   - Population: 250k + 150k | Consommation: 500k + 300k")
    print()
    print("4. 🚀 LARGE (4.5M lignes, ~200MB)")
    print("   - Tests de performance élevés")
    print("   - Population: 1M + 500k | Consommation: 2M + 1M")
    print()
    print("5. 🏗️  XLARGE (12M lignes, ~500MB)")
    print("   - Tests industriels")
    print("   - Population: 2.5M + 1.5M | Consommation: 5M + 3M")
    print()
    print("6. 🏭 XXLARGE (24M lignes, ~1GB)")
    print("   - Tests industriels massifs")
    print("   - Population: 5M + 3M | Consommation: 10M + 6M")
    print()
    print("7. 🎛️  CUSTOM")
    print("   - Configuration personnalisée")
    print()
    print("0. ❌ Quitter")
    print()

def get_user_choice():
    """Récupérer le choix de l'utilisateur."""
    
    try:
        choice = input("Votre choix (0-7): ").strip()
        return int(choice) if choice.isdigit() else -1
    except KeyboardInterrupt:
        print("\n👋 Au revoir!")
        sys.exit(0)

def run_generation(scale, custom_config=None):
    """Lancer la génération avec la configuration choisie."""
    
    print(f"\n🚀 Lancement de la génération en mode: {scale.upper()}")
    print("-" * 50)
    
    if custom_config:
        # Générer avec configuration personnalisée
        # Pour l'instant, utiliser le script standard
        cmd = ["python", "generate_large_datasets.py"]
    else:
        # Utiliser le générateur massif
        cmd = ["python", "generate_massive_datasets.py", scale]
    
    try:
        # Lancer le processus de génération
        result = subprocess.run(cmd, check=True, capture_output=False)
        
        if result.returncode == 0:
            print("\n✅ Génération terminée avec succès!")
            
            # Afficher les statistiques des fichiers générés
            show_generated_files_stats()
            
        else:
            print(f"\n❌ Erreur lors de la génération (code: {result.returncode})")
            
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Erreur lors de l'exécution: {e}")
    except FileNotFoundError:
        print("\n❌ Script de génération non trouvé!")
        print("Assurez-vous que 'generate_massive_datasets.py' existe dans le répertoire courant.")

def get_custom_config():
    """Obtenir une configuration personnalisée de l'utilisateur."""
    
    print("\n🎛️  CONFIGURATION PERSONNALISÉE")
    print("-" * 40)
    
    try:
        pop_paris = int(input("Population Paris (ex: 100000): ") or "100000")
        pop_evry = int(input("Population Évry (ex: 50000): ") or "50000")
        conso_paris = int(input("Consommation Paris (ex: 150000): ") or "150000")
        conso_evry = int(input("Consommation Évry (ex: 75000): ") or "75000")
        
        return {
            'population_paris': pop_paris,
            'population_evry': pop_evry,
            'consumption_paris': conso_paris,
            'consumption_evry': conso_evry
        }
    except ValueError:
        print("❌ Valeurs invalides saisies!")
        return None

def show_generated_files_stats():
    """Afficher les statistiques des fichiers générés."""
    
    sources_dir = Path("sources")
    if not sources_dir.exists():
        print("📁 Aucun dossier 'sources' trouvé")
        return
    
    print("\n📊 FICHIERS GÉNÉRÉS")
    print("-" * 30)
    
    total_size = 0
    total_lines = 0
    
    for csv_file in sources_dir.glob("*.csv"):
        try:
            size_mb = csv_file.stat().st_size / (1024 * 1024)
            
            # Compter les lignes (approximation rapide)
            with open(csv_file, 'r', encoding='utf-8') as f:
                lines = sum(1 for _ in f) - 1  # -1 pour le header
            
            total_size += size_mb
            total_lines += lines
            
            print(f"{csv_file.name:25} : {size_mb:6.1f} MB ({lines:,} lignes)")
            
        except Exception as e:
            print(f"{csv_file.name:25} : Erreur - {e}")
    
    print("-" * 40)
    print(f"{'TOTAL':25} : {total_size:6.1f} MB ({total_lines:,} lignes)")

def show_current_data():
    """Afficher les données actuellement présentes."""
    
    sources_dir = Path("sources")
    if not sources_dir.exists() or not any(sources_dir.glob("*.csv")):
        print("\n📁 Aucune donnée trouvée dans le dossier 'sources'")
        return
    
    print("\n📋 DONNÉES ACTUELLES")
    show_generated_files_stats()

def clean_data():
    """Nettoyer les données existantes."""
    
    sources_dir = Path("sources")
    if not sources_dir.exists():
        print("\n📁 Aucun dossier 'sources' trouvé")
        return
    
    csv_files = list(sources_dir.glob("*.csv"))
    if not csv_files:
        print("\n📁 Aucun fichier CSV trouvé dans 'sources'")
        return
    
    print(f"\n🗑️  Suppression de {len(csv_files)} fichiers...")
    
    for csv_file in csv_files:
        try:
            csv_file.unlink()
            print(f"  ✓ {csv_file.name} supprimé")
        except Exception as e:
            print(f"  ❌ Erreur lors de la suppression de {csv_file.name}: {e}")
    
    print("\n✅ Nettoyage terminé")

def run_airflow_dag():
    """Lancer le DAG Airflow pour tester les données."""
    
    sources_dir = Path("sources")
    if not sources_dir.exists() or not any(sources_dir.glob("*.csv")):
        print("\n❌ Aucune donnée trouvée! Générez d'abord des données.")
        return
    
    print("\n🚀 LANCEMENT DU DAG AIRFLOW")
    print("-" * 35)
    
    # Vérifier si Docker est disponible
    try:
        subprocess.run(["docker", "--version"], check=True, capture_output=True)
        
        print("🐳 Docker détecté")
        print("Pour lancer Airflow avec vos données:")
        print()
        print("1. docker-compose up airflow-init")
        print("2. docker-compose up -d")
        print("3. Ouvrir http://localhost:8080")
        print("4. Login: admin / admin123")
        print("5. Déclencher le DAG 'energy_data_quality_etl'")
        print()
        
        launch = input("Lancer automatiquement? (o/N): ").lower().strip()
        if launch in ['o', 'oui', 'y', 'yes']:
            subprocess.run(["docker-compose", "up", "airflow-init"])
            subprocess.run(["docker-compose", "up", "-d"])
            print("\n🌐 Airflow démarré sur http://localhost:8080")
        
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Docker non trouvé ou non démarré")
        print("Installez Docker pour utiliser Airflow automatiquement")

def main():
    """Fonction principale avec menu interactif."""
    
    while True:
        try:
            display_menu()
            
            # Afficher les données actuelles si elles existent
            sources_dir = Path("sources")
            if sources_dir.exists() and any(sources_dir.glob("*.csv")):
                print("📊 Données actuellement présentes:")
                show_current_data()
                print()
            
            choice = get_user_choice()
            
            if choice == 0:
                print("\n👋 Au revoir!")
                break
                
            elif choice == 1:  # DEMO
                print("\n⚠️  Mode DEMO: utilisation du générateur standard")
                try:
                    subprocess.run(["python", "generate_large_datasets.py", "0.3"], check=True)
                    show_generated_files_stats()
                except Exception as e:
                    print(f"❌ Erreur: {e}")
                
            elif choice == 2:  # SMALL
                run_generation("small")
                
            elif choice == 3:  # MEDIUM
                run_generation("medium")
                
            elif choice == 4:  # LARGE
                run_generation("large")
                
            elif choice == 5:  # XLARGE
                run_generation("xlarge")
                
            elif choice == 6:  # XXLARGE
                run_generation("xxlarge")
                
            elif choice == 7:  # CUSTOM
                custom = get_custom_config()
                if custom:
                    run_generation("custom", custom)
                    
            else:
                print("\n❌ Choix invalide!")
                continue
            
            # Menu post-génération
            print("\n" + "="*50)
            print("🎯 QUE VOULEZ-VOUS FAIRE MAINTENANT?")
            print("1. 🔄 Générer d'autres données")
            print("2. 🚀 Lancer Airflow pour tester l'ETL")
            print("3. 🗑️  Nettoyer les données")
            print("4. 📊 Voir les statistiques")
            print("5. ❌ Quitter")
            
            next_action = input("\nVotre choix (1-5): ").strip()
            
            if next_action == "1":
                continue
            elif next_action == "2":
                run_airflow_dag()
            elif next_action == "3":
                clean_data()
            elif next_action == "4":
                show_current_data()
            elif next_action == "5":
                break
            else:
                print("Choix invalide, retour au menu principal...")
            
            input("\nAppuyez sur Entrée pour continuer...")
            
        except KeyboardInterrupt:
            print("\n\n👋 Au revoir!")
            break
        except Exception as e:
            print(f"\n❌ Erreur inattendue: {e}")
            input("Appuyez sur Entrée pour continuer...")

if __name__ == "__main__":
    main()