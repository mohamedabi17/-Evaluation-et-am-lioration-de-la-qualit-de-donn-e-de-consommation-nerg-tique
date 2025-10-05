#!/usr/bin/env python3
"""
🚀 INSTALLATION AIRFLOW SIMPLIFIÉE
Installation d'Airflow compatible avec Python 3.13
"""

import os
import sys
import subprocess
import time
from pathlib import Path

def install_airflow_simple():
    """Installer Airflow de manière simple."""
    
    print("🔧 INSTALLATION SIMPLIFIÉE D'AIRFLOW")
    print("=" * 50)
    
    # Configuration AIRFLOW_HOME
    airflow_home = Path.home() / "airflow"
    os.environ['AIRFLOW_HOME'] = str(airflow_home)
    
    print(f"📁 AIRFLOW_HOME: {airflow_home}")
    
    try:
        # Installation sans contraintes de version (plus flexible)
        print("📦 Installation d'Airflow (version compatible)...")
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            "apache-airflow",
            "--no-deps"  # Éviter les conflits de dépendances
        ])
        
        # Installer les dépendances essentielles séparément
        print("📦 Installation des dépendances...")
        essential_deps = [
            "flask", "werkzeug", "wtforms", "jinja2", 
            "sqlalchemy", "alembic", "croniter", "pendulum"
        ]
        
        for dep in essential_deps:
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", dep])
            except:
                print(f"⚠️ Erreur avec {dep}, continuons...")
        
        print("✅ Airflow installé")
        
        # Initialisation simplifiée
        print("🗄️ Initialisation...")
        try:
            subprocess.check_call([sys.executable, "-m", "airflow", "db", "init"])
            print("✅ Base de données initialisée")
        except:
            print("⚠️ Erreur initialisation DB, continuons...")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

def create_quick_airflow_script():
    """Créer un script de démarrage rapide."""
    
    print("\n📝 CRÉATION DU SCRIPT DE DÉMARRAGE")
    print("-" * 40)
    
    # Script Python simple pour démarrer Airflow
    airflow_script = '''#!/usr/bin/env python3
"""
Démarrage rapide d'Airflow sur localhost:8080
"""

import os
import sys
import subprocess
import time
from pathlib import Path

def start_airflow():
    """Démarrer Airflow en mode standalone."""
    
    # Configuration
    airflow_home = Path.home() / "airflow"
    os.environ['AIRFLOW_HOME'] = str(airflow_home)
    
    print("🚀 DÉMARRAGE D'AIRFLOW STANDALONE")
    print("=" * 45)
    print(f"📁 AIRFLOW_HOME: {airflow_home}")
    print("🌐 URL: http://localhost:8080")
    print("👤 Utilisateur: admin")
    print("🔑 Mot de passe: admin")
    print()
    
    try:
        # Mode standalone (tout-en-un)
        print("⚡ Lancement en mode standalone...")
        subprocess.run([
            sys.executable, "-m", "airflow", "standalone"
        ])
        
    except KeyboardInterrupt:
        print("\\n🛑 Arrêt d'Airflow")
    except Exception as e:
        print(f"❌ Erreur: {e}")

if __name__ == "__main__":
    start_airflow()
'''
    
    with open("start_airflow_standalone.py", "w") as f:
        f.write(airflow_script)
    
    print("✅ Script créé: start_airflow_standalone.py")

def create_manual_instructions():
    """Créer des instructions manuelles."""
    
    instructions = """
🚀 INSTRUCTIONS POUR DÉMARRER AIRFLOW

# MÉTHODE 1: Installation manuelle (recommandée)
pip install apache-airflow
export AIRFLOW_HOME=~/airflow  # Linux/Mac
set AIRFLOW_HOME=%USERPROFILE%\\airflow  # Windows
airflow db init
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
airflow standalone

# MÉTHODE 2: Utiliser le script
python start_airflow_standalone.py

# MÉTHODE 3: Docker (si disponible)
docker run -p 8080:8080 apache/airflow:2.8.1 standalone

# ACCÈS
URL: http://localhost:8080
Utilisateur: admin
Mot de passe: admin

# COPIER VOTRE DAG
Copiez le fichier dags/energy_quality_etl.py vers ~/airflow/dags/
"""
    
    with open("AIRFLOW_INSTRUCTIONS.txt", "w") as f:
        f.write(instructions)
    
    print("✅ Instructions créées: AIRFLOW_INSTRUCTIONS.txt")

def main():
    """Fonction principale."""
    
    print("🎯 INSTALLATION AIRFLOW SIMPLIFIÉE")
    print("=" * 50)
    print()
    
    # Vérifier Python
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    print(f"🐍 Python: {python_version}")
    
    if sys.version_info >= (3, 13):
        print("⚠️ Python 3.13+ détecté - installation alternative")
    
    # Proposer différentes options
    print(f"\n📋 OPTIONS D'INSTALLATION:")
    print("1. Installation automatique (peut échouer avec Python 3.13)")
    print("2. Créer seulement les scripts et instructions")
    print("3. Installation manuelle guidée")
    
    choice = input("\\nChoisissez (1/2/3): ").strip()
    
    if choice == "1":
        if install_airflow_simple():
            create_quick_airflow_script()
            print(f"\\n🎉 INSTALLATION RÉUSSIE!")
            print("▶️ Exécutez: python start_airflow_standalone.py")
        else:
            print("❌ Échec - essayez l'option 2 ou 3")
    
    elif choice == "2":
        create_quick_airflow_script()
        create_manual_instructions()
        
        print(f"\\n📋 SCRIPTS CRÉÉS!")
        print("📄 Consultez: AIRFLOW_INSTRUCTIONS.txt")
        print("🚀 Ou lancez: python start_airflow_standalone.py")
    
    elif choice == "3":
        print(f"\\n📋 INSTALLATION MANUELLE:")
        print("1. Ouvrez un terminal")
        print("2. pip install apache-airflow")
        print("3. set AIRFLOW_HOME=%USERPROFILE%\\\\airflow")
        print("4. airflow db init")
        print("5. airflow users create --username admin --password admin --role Admin --email admin@example.com --firstname Admin --lastname User")
        print("6. airflow standalone")
        print("7. Ouvrez http://localhost:8080")
        
        create_manual_instructions()
    
    print(f"\\n🎯 ÉTAPES SUIVANTES:")
    print("1. Démarrer Airflow sur localhost:8080")
    print("2. Copier votre DAG dans ~/airflow/dags/")
    print("3. Actualiser l'interface web")
    print("4. Exécuter le DAG 'energy_quality_etl'")

if __name__ == "__main__":
    main()
'''

def main():
    """Fonction principale."""
    
    print("🎯 SETUP AIRFLOW POUR LOCALHOST")
    print("=" * 40)
    
    create_quick_airflow_script()
    create_manual_instructions()
    
    print("\\n🚀 DÉMARRAGE RAPIDE:")
    print("1. python start_airflow_standalone.py")
    print("2. Ouvrir http://localhost:8080")
    print("3. Connexion: admin / admin")

if __name__ == "__main__":
    main()