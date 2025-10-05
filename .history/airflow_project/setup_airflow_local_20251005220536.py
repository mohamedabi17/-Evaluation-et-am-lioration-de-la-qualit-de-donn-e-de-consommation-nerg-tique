#!/usr/bin/env python3
"""
🚀 INSTALLATION ET LANCEMENT D'AIRFLOW LOCAL
Configure et démarre Airflow sur localhost:8080
"""

import os
import sys
import subprocess
import time
from pathlib import Path

def install_airflow():
    """Installer Apache Airflow localement."""
    
    print("🔧 INSTALLATION D'APACHE AIRFLOW")
    print("=" * 50)
    
    # Variables d'environnement pour Airflow
    airflow_home = Path.home() / "airflow"
    os.environ['AIRFLOW_HOME'] = str(airflow_home)
    
    print(f"📁 AIRFLOW_HOME: {airflow_home}")
    
    try:
        # Installer Airflow avec les contraintes appropriées
        print("📦 Installation d'Airflow...")
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        constraint_url = f"https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-{python_version}.txt"
        
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            f"apache-airflow==2.8.1", 
            "--constraint", constraint_url
        ])
        
        print("✅ Airflow installé avec succès")
        
        # Initialiser la base de données
        print("🗄️ Initialisation de la base de données...")
        subprocess.check_call([
            sys.executable, "-m", "airflow", "db", "init"
        ])
        
        print("✅ Base de données initialisée")
        
        # Créer un utilisateur admin
        print("👤 Création de l'utilisateur admin...")
        subprocess.check_call([
            sys.executable, "-m", "airflow", "users", "create",
            "--username", "admin",
            "--firstname", "Admin",
            "--lastname", "User", 
            "--role", "Admin",
            "--email", "admin@example.com",
            "--password", "admin"
        ])
        
        print("✅ Utilisateur admin créé (admin/admin)")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'installation: {e}")
        return False
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}")
        return False

def setup_dags_folder():
    """Configurer le dossier des DAGs."""
    
    print("\n📁 CONFIGURATION DES DAGS")
    print("-" * 30)
    
    airflow_home = Path.home() / "airflow"
    dags_folder = airflow_home / "dags"
    
    # Créer le dossier dags s'il n'existe pas
    dags_folder.mkdir(exist_ok=True)
    
    # Copier notre DAG dans le dossier Airflow
    project_dag = Path("dags/energy_quality_etl.py")
    target_dag = dags_folder / "energy_quality_etl.py"
    
    if project_dag.exists():
        import shutil
        shutil.copy2(project_dag, target_dag)
        print(f"✅ DAG copié: {target_dag}")
        
        # Copier aussi les utilitaires
        utils_target = dags_folder / "utils"
        utils_source = Path("utils")
        
        if utils_source.exists():
            if utils_target.exists():
                shutil.rmtree(utils_target)
            shutil.copytree(utils_source, utils_target)
            print(f"✅ Utilitaires copiés: {utils_target}")
        
        # Copier les sources de données
        sources_target = dags_folder / "sources"
        sources_source = Path("sources")
        
        if sources_source.exists():
            if sources_target.exists():
                shutil.rmtree(sources_target)
            shutil.copytree(sources_source, sources_target)
            print(f"✅ Sources copiées: {sources_target}")
        
        return True
    else:
        print(f"❌ DAG non trouvé: {project_dag}")
        return False

def start_airflow_webserver():
    """Démarrer le serveur web Airflow."""
    
    print("\n🌐 DÉMARRAGE DU SERVEUR WEB AIRFLOW")
    print("-" * 45)
    
    try:
        print("🚀 Lancement du serveur web sur localhost:8080...")
        print("⏳ Patientez quelques secondes pour l'initialisation...")
        
        # Démarrer le serveur web en arrière-plan
        process = subprocess.Popen([
            sys.executable, "-m", "airflow", "webserver", 
            "--port", "8080"
        ])
        
        print(f"✅ Serveur web démarré (PID: {process.pid})")
        print(f"🌐 URL: http://localhost:8080")
        print(f"👤 Connexion: admin / admin")
        
        return process
        
    except Exception as e:
        print(f"❌ Erreur démarrage serveur web: {e}")
        return None

def start_airflow_scheduler():
    """Démarrer le scheduler Airflow."""
    
    print("\n⚡ DÉMARRAGE DU SCHEDULER AIRFLOW")
    print("-" * 40)
    
    try:
        print("🔄 Lancement du scheduler...")
        
        # Démarrer le scheduler en arrière-plan
        process = subprocess.Popen([
            sys.executable, "-m", "airflow", "scheduler"
        ])
        
        print(f"✅ Scheduler démarré (PID: {process.pid})")
        
        return process
        
    except Exception as e:
        print(f"❌ Erreur démarrage scheduler: {e}")
        return None

def check_airflow_status():
    """Vérifier le statut d'Airflow."""
    
    print("\n🔍 VÉRIFICATION DU STATUT")
    print("-" * 30)
    
    try:
        import requests
        
        # Tester la connexion au serveur web
        response = requests.get("http://localhost:8080/health", timeout=5)
        if response.status_code == 200:
            print("✅ Serveur web accessible")
        else:
            print(f"⚠️ Serveur web répond avec code {response.status_code}")
            
    except requests.exceptions.RequestException:
        print("⚠️ Serveur web pas encore accessible (normal au démarrage)")
    except ImportError:
        print("📦 Installation de requests pour les tests...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])

def create_startup_script():
    """Créer un script de démarrage rapide."""
    
    startup_script = """@echo off
echo 🚀 DÉMARRAGE RAPIDE AIRFLOW
echo ========================

echo 📁 Configuration AIRFLOW_HOME...
set AIRFLOW_HOME=%USERPROFILE%\\airflow

echo 🌐 Démarrage serveur web...
start "Airflow Webserver" python -m airflow webserver --port 8080

echo ⏳ Attente 10 secondes...
timeout /t 10 /nobreak

echo ⚡ Démarrage scheduler...
start "Airflow Scheduler" python -m airflow scheduler

echo ✅ Airflow démarré!
echo 🌐 Interface web: http://localhost:8080
echo 👤 Connexion: admin / admin

pause
"""
    
    with open("start_airflow.bat", "w") as f:
        f.write(startup_script)
    
    print("✅ Script de démarrage créé: start_airflow.bat")

def main():
    """Fonction principale."""
    
    print("🚀 CONFIGURATION AIRFLOW LOCAL")
    print("=" * 60)
    print("Installation et configuration d'Apache Airflow sur localhost")
    print()
    
    # Vérifier qu'on est dans le bon répertoire
    if not Path("dags").exists():
        print("❌ Erreur: Exécutez ce script depuis le répertoire du projet")
        return
    
    # Étape 1: Installation
    if not install_airflow():
        print("❌ Échec de l'installation")
        return
    
    # Étape 2: Configuration des DAGs
    if not setup_dags_folder():
        print("❌ Échec de la configuration des DAGs")
        return
    
    # Étape 3: Créer le script de démarrage
    create_startup_script()
    
    # Étape 4: Proposer le démarrage
    print(f"\n🎯 INSTALLATION TERMINÉE")
    print("=" * 30)
    print("📋 Prochaines étapes:")
    print("   1. Fermer ce terminal")
    print("   2. Ouvrir 2 nouveaux terminaux")
    print("   3. Terminal 1: python -m airflow webserver --port 8080")
    print("   4. Terminal 2: python -m airflow scheduler")
    print("   5. Ouvrir http://localhost:8080 (admin/admin)")
    print()
    
    response = input("🚀 Démarrer Airflow maintenant? (y/n): ").strip().lower()
    
    if response == 'y':
        print("\n🚀 DÉMARRAGE D'AIRFLOW...")
        
        # Démarrer le serveur web
        webserver_process = start_airflow_webserver()
        
        if webserver_process:
            time.sleep(5)  # Attendre un peu
            
            # Démarrer le scheduler
            scheduler_process = start_airflow_scheduler()
            
            if scheduler_process:
                time.sleep(3)
                check_airflow_status()
                
                print(f"\n🎉 AIRFLOW DÉMARRÉ AVEC SUCCÈS!")
                print("=" * 40)
                print("🌐 Interface web: http://localhost:8080")
                print("👤 Connexion: admin / admin")
                print("📊 Votre DAG 'energy_quality_etl' devrait être visible")
                print()
                print("⚠️ Laissez ces processus tourner en arrière-plan")
                print("🛑 Pour arrêter: Ctrl+C dans chaque terminal")
                
                # Proposer d'ouvrir le navigateur
                try:
                    import webbrowser
                    open_browser = input("🌐 Ouvrir dans le navigateur? (y/n): ").strip().lower()
                    if open_browser == 'y':
                        webbrowser.open("http://localhost:8080")
                except:
                    pass
                
                print("\n✅ Configuration terminée!")
                print("👋 Vous pouvez maintenant utiliser Airflow sur localhost:8080")

if __name__ == "__main__":
    main()