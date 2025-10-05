#!/usr/bin/env python3
"""
Script de démarrage rapide pour le projet ETL Qualité des Données
Interface unifiée pour générer, analyser et traiter les données
"""

import os
import sys
import subprocess
from pathlib import Path
import json

def show_banner():
    """Afficher la bannière du projet."""
    print("🎯" + "="*58 + "🎯")
    print("🚀 PROJET ETL QUALITÉ DES DONNÉES ÉNERGÉTIQUES - AIRFLOW 🚀")
    print("🎯" + "="*58 + "🎯")
    print()
    print("📊 Ce projet démontre un pipeline ETL complet avec:")
    print("   ✓ Évaluation de qualité des données")
    print("   ✓ Amélioration automatique des défauts")
    print("   ✓ Intégration multi-sources")
    print("   ✓ Visualisations et rapports")
    print("   ✓ Tests de performance")
    print()

def check_dependencies():
    """Vérifier les dépendances système."""
    print("🔍 Vérification des dépendances...")
    
    # Python packages
    required_packages = ['pandas', 'numpy', 'matplotlib', 'seaborn', 'psutil']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ Packages Python manquants: {', '.join(missing_packages)}")
        print("💡 Installez-les avec: pip install -r requirements.txt")
        return False
    
    # Docker (optionnel)
    try:
        subprocess.run(['docker', '--version'], check=True, capture_output=True)
        print("✅ Docker disponible")
        docker_available = True
    except:
        print("⚠️  Docker non détecté (optionnel pour Airflow)")
        docker_available = False
    
    print("✅ Dépendances vérifiées")
    return True

def show_data_status():
    """Afficher le statut des données."""
    sources_dir = Path("sources")
    
    if not sources_dir.exists() or not any(sources_dir.glob("*.csv")):
        print("📂 Statut des données: ❌ AUCUNE DONNÉE")
        return False
    
    print("📂 Statut des données: ✅ DONNÉES PRÉSENTES")
    
    total_size = 0
    total_lines = 0
    
    for csv_file in sources_dir.glob("*.csv"):
        size_mb = csv_file.stat().st_size / (1024**2)
        total_size += size_mb
        
        # Estimation rapide du nombre de lignes
        try:
            with open(csv_file, 'r') as f:
                lines = sum(1 for _ in f) - 1  # -1 pour header
            total_lines += lines
        except:
            lines = 0
        
        print(f"   {csv_file.name:25} : {size_mb:6.1f} MB ({lines:,} lignes)")
    
    print(f"   {'TOTAL':25} : {total_size:6.1f} MB ({total_lines:,} lignes)")
    return True

def main_menu():
    """Afficher le menu principal."""
    
    data_exists = show_data_status()
    print()
    
    print("🎛️  QUE VOULEZ-VOUS FAIRE?")
    print("-" * 30)
    
    if not data_exists:
        print("1. 📊 Générer des données synthétiques")
        print("2. 📋 Voir la documentation")
        print("0. ❌ Quitter")
        
        choice = input("\nVotre choix (0-2): ").strip()
        
        if choice == "1":
            generate_data_menu()
        elif choice == "2":
            show_documentation()
        elif choice == "0":
            print("👋 Au revoir!")
            return False
        else:
            print("❌ Choix invalide")
            
    else:
        print("1. 📊 Générer de nouvelles données")
        print("2. 🔍 Analyser la qualité des données")
        print("3. 🧹 Démonstration d'amélioration qualité")
        print("4. 📈 Benchmark de performance")
        print("5. 🚀 Lancer Airflow (ETL complet)")
        print("6. 📋 Voir la documentation")
        print("7. 🗑️  Nettoyer les données")
        print("0. ❌ Quitter")
        
        choice = input("\nVotre choix (0-7): ").strip()
        
        if choice == "1":
            generate_data_menu()
        elif choice == "2":
            analyze_data_quality()
        elif choice == "3":
            demo_quality_improvement()
        elif choice == "4":
            run_performance_benchmark()
        elif choice == "5":
            launch_airflow()
        elif choice == "6":
            show_documentation()
        elif choice == "7":
            clean_data()
        elif choice == "0":
            print("👋 Au revoir!")
            return False
        else:
            print("❌ Choix invalide")
    
    return True

def generate_data_menu():
    """Menu de génération de données."""
    print("\n📊 GÉNÉRATION DE DONNÉES")
    print("-" * 30)
    print("1. 🔧 Configuration automatique (recommandé)")
    print("2. 📈 Générateur standard")
    print("3. 🏭 Générateur massif")
    print("0. ⬅️  Retour")
    
    choice = input("\nVotre choix (0-3): ").strip()
    
    if choice == "1":
        try:
            subprocess.run([sys.executable, "configure_datasets.py"])
        except Exception as e:
            print(f"❌ Erreur: {e}")
    elif choice == "2":
        try:
            subprocess.run([sys.executable, "generate_large_datasets.py"])
        except Exception as e:
            print(f"❌ Erreur: {e}")
    elif choice == "3":
        try:
            subprocess.run([sys.executable, "generate_massive_datasets.py"])
        except Exception as e:
            print(f"❌ Erreur: {e}")

def analyze_data_quality():
    """Analyser la qualité des données."""
    print("\n🔍 ANALYSE DE QUALITÉ DES DONNÉES")
    print("-" * 40)
    
    try:
        # Importer et utiliser les outils de qualité
        sys.path.append('utils')
        from quality_checks import generate_quality_report
        import pandas as pd
        
        sources_dir = Path("sources")
        
        for csv_file in sources_dir.glob("*.csv"):
            if csv_file.name.startswith(('population', 'consommation')):
                print(f"📊 Analyse de {csv_file.name}...")
                
                df = pd.read_csv(csv_file)
                report = generate_quality_report(df, csv_file.stem)
                
                score = report['quality_score']['overall_quality_score']
                level = report['quality_score']['quality_level']
                
                print(f"   Score global: {score}% ({level})")
                print(f"   Lignes: {len(df):,}")
                print(f"   Colonnes: {len(df.columns)}")
                
                # Problèmes principaux
                completeness = report['quality_score']['completeness_score']
                uniqueness = report['quality_score']['uniqueness_score']
                
                if completeness < 95:
                    print(f"   ⚠️  Complétude faible: {completeness}%")
                if uniqueness < 95:
                    print(f"   ⚠️  Doublons détectés: {100-uniqueness:.1f}%")
                
                print()
        
        print("✅ Analyse terminée")
        print("📊 Rapports détaillés disponibles dans 'quality_reports/'")
        
    except Exception as e:
        print(f"❌ Erreur lors de l'analyse: {e}")

def demo_quality_improvement():
    """Démonstration d'amélioration de qualité."""
    print("\n🧹 DÉMONSTRATION D'AMÉLIORATION DE QUALITÉ")
    print("-" * 50)
    
    try:
        subprocess.run([sys.executable, "demo_quality_improvement.py"])
    except Exception as e:
        print(f"❌ Erreur: {e}")

def run_performance_benchmark():
    """Lancer le benchmark de performance."""
    print("\n📈 BENCHMARK DE PERFORMANCE")
    print("-" * 35)
    
    try:
        subprocess.run([sys.executable, "benchmark_performance.py"])
    except Exception as e:
        print(f"❌ Erreur: {e}")

def launch_airflow():
    """Lancer Airflow."""
    print("\n🚀 LANCEMENT D'AIRFLOW")
    print("-" * 25)
    
    # Vérifier Docker
    try:
        subprocess.run(['docker', '--version'], check=True, capture_output=True)
        
        print("🐳 Docker détecté")
        print("📋 Instructions pour lancer Airflow:")
        print()
        print("1. Initialiser Airflow:")
        print("   docker-compose up airflow-init")
        print()
        print("2. Démarrer Airflow:")
        print("   docker-compose up -d")
        print()
        print("3. Accéder à l'interface:")
        print("   http://localhost:8080")
        print("   Login: admin / admin123")
        print()
        print("4. Déclencher le DAG 'energy_data_quality_etl'")
        print()
        
        auto_launch = input("Lancer automatiquement? (o/N): ").lower().strip()
        
        if auto_launch in ['o', 'oui', 'y', 'yes']:
            print("🚀 Lancement d'Airflow...")
            try:
                subprocess.run(['docker-compose', 'up', 'airflow-init'], check=True)
                subprocess.run(['docker-compose', 'up', '-d'], check=True)
                print("✅ Airflow démarré sur http://localhost:8080")
            except Exception as e:
                print(f"❌ Erreur lors du lancement: {e}")
        
    except:
        print("❌ Docker non disponible")
        print("📋 Alternative: Installation locale d'Airflow")
        print("   pip install apache-airflow")
        print("   export AIRFLOW_HOME=$(pwd)")
        print("   airflow db init")
        print("   airflow webserver --port 8080")

def clean_data():
    """Nettoyer les données."""
    print("\n🗑️  NETTOYAGE DES DONNÉES")
    print("-" * 30)
    
    sources_dir = Path("sources")
    output_dir = Path("output")
    reports_dir = Path("quality_reports")
    viz_dir = Path("visualizations")
    
    dirs_to_clean = [sources_dir, output_dir, reports_dir, viz_dir]
    
    for directory in dirs_to_clean:
        if directory.exists():
            files = list(directory.glob("*"))
            if files:
                print(f"📁 {directory.name}: {len(files)} fichiers trouvés")
    
    confirm = input("\nConfirmer la suppression? (o/N): ").lower().strip()
    
    if confirm in ['o', 'oui', 'y', 'yes']:
        for directory in dirs_to_clean:
            if directory.exists():
                for file in directory.glob("*"):
                    if file.is_file():
                        file.unlink()
                        print(f"  ✓ {file.name} supprimé")
        print("✅ Nettoyage terminé")
    else:
        print("❌ Nettoyage annulé")

def show_documentation():
    """Afficher la documentation."""
    print("\n📋 DOCUMENTATION DU PROJET")
    print("-" * 35)
    print()
    print("📁 Structure du projet:")
    print("   airflow_project/")
    print("   ├── dags/                    # DAGs Airflow")
    print("   ├── sources/                 # Données sources")
    print("   ├── output/                  # Données traitées")
    print("   ├── quality_reports/         # Rapports qualité")
    print("   ├── visualizations/          # Graphiques")
    print("   ├── utils/                   # Outils qualité")
    print("   └── README.md                # Documentation complète")
    print()
    print("🔧 Scripts disponibles:")
    print("   configure_datasets.py        # Interface de génération")
    print("   generate_large_datasets.py   # Générateur standard")
    print("   generate_massive_datasets.py # Générateur massif")
    print("   demo_quality_improvement.py  # Démo amélioration")
    print("   benchmark_performance.py     # Tests performance")
    print()
    print("📊 Schéma cible:")
    print("   Consommation_IRIS_Paris      # Consommation par zone IRIS")
    print("   Consommation_IRIS_Evry       # Consommation par zone IRIS")
    print("   Consommation_CSP             # Consommation par catégorie socio-pro")
    print()
    print("🔍 Problèmes de qualité traités:")
    print("   ✓ Valeurs manquantes")
    print("   ✓ Doublons")
    print("   ✓ Formats incohérents")
    print("   ✓ Codifications mixtes")
    print("   ✓ Hétérogénéité des échelles")
    print()
    
    readme_path = Path("README.md")
    if readme_path.exists():
        view_readme = input("Ouvrir le README.md complet? (o/N): ").lower().strip()
        if view_readme in ['o', 'oui', 'y', 'yes']:
            try:
                with open(readme_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                print("\n" + "="*60)
                print(content)
                print("="*60)
            except Exception as e:
                print(f"❌ Erreur lors de la lecture: {e}")

def main():
    """Fonction principale."""
    show_banner()
    
    # Vérifier les dépendances
    if not check_dependencies():
        print("\n❌ Veuillez installer les dépendances manquantes avant de continuer")
        return
    
    print()
    
    # Boucle principale
    while True:
        try:
            if not main_menu():
                break
            
            input("\nAppuyez sur Entrée pour continuer...")
            print("\n" + "="*60 + "\n")
            
        except KeyboardInterrupt:
            print("\n\n👋 Au revoir!")
            break
        except Exception as e:
            print(f"\n❌ Erreur inattendue: {e}")
            input("Appuyez sur Entrée pour continuer...")

if __name__ == "__main__":
    main()