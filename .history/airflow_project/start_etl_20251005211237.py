#!/usr/bin/env python3
"""
🚀 QUICK START - Démarrage rapide du projet ETL
Lance automatiquement tout le pipeline sans Docker
"""

import os
import sys
import subprocess
from pathlib import Path

def main():
    """Lancement automatique du pipeline ETL complet."""
    
    print("🎯 DÉMARRAGE AUTOMATIQUE DU PIPELINE ETL")
    print("=" * 50)
    print()
    
    # Changer vers le répertoire du projet
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    try:
        # Étape 1: Vérifier/installer les dépendances
        print("🔍 1. Vérification des dépendances Python...")
        required_packages = ['pandas', 'numpy', 'matplotlib', 'seaborn', 'scipy']
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                print(f"   Installation de {package}...")
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
        
        print("✅ Dépendances installées")
        
        # Étape 2: Générer les données si nécessaire
        print("\n📁 2. Vérification des données sources...")
        
        sources_dir = Path("sources")
        required_files = ['population_paris.csv', 'consommation_paris.csv']
        
        if not all((sources_dir / f).exists() for f in required_files):
            print("   Génération des données de test...")
            if Path("generate_large_datasets.py").exists():
                result = subprocess.run([sys.executable, "generate_large_datasets.py"])
                if result.returncode != 0:
                    raise Exception("Échec génération des données")
            else:
                raise Exception("Script de génération non trouvé")
        
        print("✅ Données sources prêtes")
        
        # Étape 3: Lancer l'ETL
        print("\n🚀 3. Lancement du pipeline ETL...")
        
        if Path("run_etl_local.py").exists():
            result = subprocess.run([sys.executable, "run_etl_local.py"])
            if result.returncode != 0:
                raise Exception("Échec de l'ETL")
        else:
            raise Exception("Script ETL non trouvé")
        
        print("\n✅ PIPELINE TERMINÉ AVEC SUCCÈS!")
        print("=" * 50)
        print("\n📂 Résultats disponibles dans:")
        print("   📊 output/               (données finales)")
        print("   📈 quality_reports/      (rapports qualité)")  
        print("   📉 visualizations/       (graphiques)")
        
        # Afficher un aperçu des résultats
        output_dir = Path("output")
        if output_dir.exists():
            csv_files = list(output_dir.glob("*.csv"))
            if csv_files:
                print(f"\n📋 {len(csv_files)} fichiers créés:")
                for f in csv_files:
                    size_kb = f.stat().st_size / 1024
                    print(f"   📄 {f.name:30} ({size_kb:.1f} KB)")
        
        print("\n🎉 Votre évaluation de qualité des données est terminée!")
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        print("\n💡 DÉPANNAGE MANUEL:")
        print("1. Vérifiez Python 3.8+: python --version")
        print("2. Installez les packages: pip install pandas numpy matplotlib seaborn scipy")
        print("3. Générez les données: python generate_large_datasets.py")
        print("4. Lancez l'ETL: python run_etl_local.py")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)