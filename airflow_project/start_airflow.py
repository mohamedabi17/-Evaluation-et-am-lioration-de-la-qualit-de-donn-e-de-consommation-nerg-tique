#!/usr/bin/env python3
"""
🚀 DÉMARRAGE RAPIDE AIRFLOW
Script pour lancer Airflow sur localhost:8080
"""

import os
import sys
import subprocess
from pathlib import Path

def setup_airflow_env():
    """Configurer l'environnement Airflow."""
    airflow_home = Path.home() / "airflow"
    os.environ['AIRFLOW_HOME'] = str(airflow_home)
    return airflow_home

def install_airflow():
    """Installer Airflow."""
    print("📦 Installation d'Apache Airflow...")
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "apache-airflow"
        ])
        return True
    except:
        print("⚠️ Erreur d'installation, continuons...")
        return False

def init_airflow():
    """Initialiser Airflow."""
    print("🗄️ Initialisation de la base de données...")
    try:
        subprocess.check_call([sys.executable, "-m", "airflow", "db", "init"])
        return True
    except:
        print("⚠️ Erreur d'initialisation, continuons...")
        return False

def create_admin_user():
    """Créer l'utilisateur admin."""
    print("👤 Création de l'utilisateur admin...")
    try:
        subprocess.check_call([
            sys.executable, "-m", "airflow", "users", "create",
            "--username", "admin",
            "--password", "admin", 
            "--firstname", "Admin",
            "--lastname", "User",
            "--role", "Admin",
            "--email", "admin@example.com"
        ])
        return True
    except:
        print("⚠️ Utilisateur peut-être déjà existant...")
        return False

def start_airflow():
    """Démarrer Airflow en mode standalone."""
    print("🚀 Démarrage d'Airflow...")
    print("🌐 URL: http://localhost:8080")
    print("👤 Connexion: admin / admin")
    print("⏳ Patientez pour le démarrage...")
    print()
    
    try:
        subprocess.run([sys.executable, "-m", "airflow", "standalone"])
    except KeyboardInterrupt:
        print("\n🛑 Arrêt d'Airflow")

def main():
    """Fonction principale."""
    print("🚀 LANCEMENT D'AIRFLOW SUR LOCALHOST")
    print("=" * 50)
    
    # Configuration
    airflow_home = setup_airflow_env()
    print(f"📁 AIRFLOW_HOME: {airflow_home}")
    
    # Installation et configuration
    install_airflow()
    init_airflow() 
    create_admin_user()
    
    print("\n🎯 PRÊT À DÉMARRER!")
    print("=" * 25)
    
    # Démarrage
    start_airflow()

if __name__ == "__main__":
    main()