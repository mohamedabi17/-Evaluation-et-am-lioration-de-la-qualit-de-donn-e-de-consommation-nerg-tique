#!/usr/bin/env python3
"""
Générateur de TRÈS GRANDES quantités de données (Scale : Millions de lignes)
Ce script peut générer des datasets de taille industrielle pour tester les performances ETL
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

# Configuration pour TRÈS GROS datasets
SCALE_CONFIGS = {
    'small': {
        'population_paris': 50_000,
        'population_evry': 25_000,
        'consumption_paris': 75_000,
        'consumption_evry': 35_000,
        'description': 'Configuration standard (~190k lignes, ~8MB)'
    },
    'medium': {
        'population_paris': 250_000,
        'population_evry': 150_000,
        'consumption_paris': 500_000,
        'consumption_evry': 300_000,
        'description': 'Configuration moyenne (~1.2M lignes, ~50MB)'
    },
    'large': {
        'population_paris': 1_000_000,
        'population_evry': 500_000,
        'consumption_paris': 2_000_000,
        'consumption_evry': 1_000_000,
        'description': 'Configuration importante (~4.5M lignes, ~200MB)'
    },
    'xlarge': {
        'population_paris': 2_500_000,
        'population_evry': 1_500_000,
        'consumption_paris': 5_000_000,
        'consumption_evry': 3_000_000,
        'description': 'Configuration très importante (~12M lignes, ~500MB)'
    },
    'xxlarge': {
        'population_paris': 5_000_000,
        'population_evry': 3_000_000,
        'consumption_paris': 10_000_000,
        'consumption_evry': 6_000_000,
        'description': 'Configuration industrielle (~24M lignes, ~1GB)'
    }
}

def generate_massive_population(city, size, chunk_size=50000):
    """Générer une population massive par chunks pour éviter les problèmes de mémoire."""
    
    print(f"👥 Génération MASSIVE de {size:,} habitants pour {city} (par chunks de {chunk_size:,})...")
    
    # Générateur de noms/prénoms avec recyclage pour optimiser la mémoire
    noms_cycle = random.choices([
        "Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand",
        "Leroy", "Moreau", "Simon", "Laurent", "Lefebvre", "Michel", "Garcia", "David",
        "Bertrand", "Roux", "Vincent", "Fournier", "Nguyen", "Lopez", "Gonzalez", "Rodriguez",
        "Chen", "Wang", "Li", "Zhang", "Kim", "Park", "Johnson", "Williams", "Brown", "Jones"
    ], k=1000)
    
    prenoms_cycle = random.choices([
        "Jean", "Marie", "Michel", "Alain", "Patrick", "Pierre", "Philippe", "Christophe",
        "Sophie", "Julie", "Camille", "Emma", "Lea", "Manon", "Lucas", "Hugo", "Louis",
        "Gabriel", "Arthur", "Raphael", "Mohamed", "Ahmed", "Hassan", "Fatima", "Aicha",
        "Sarah", "Leila", "David", "Daniel", "Michael", "Kevin", "Anthony", "Lisa"
    ], k=1000)
    
    # Adresses pré-générées pour éviter la répétition
    if city.lower() == "paris":
        rues_base = [
            "Rue de Rivoli", "Avenue des Champs-Elysées", "Boulevard Saint-Germain",
            "Rue de la Paix", "Avenue Montaigne", "Rue du Faubourg Saint-Honoré",
            "Boulevard Haussmann", "Rue Lafayette", "Avenue Victor Hugo", "Rue Saint-Antoine"
        ]
        id_offset = 0
    else:
        rues_base = [
            "Rue de l'Essonne", "Avenue de la Gare", "Boulevard des Champs",
            "Rue de la République", "Avenue du Parc", "Boulevard Liberté",
            "Avenue Jean Jaurès", "Rue Nationale", "Avenue François Mitterrand"
        ]
        id_offset = 10_000_000
    
    # Élargir la liste des rues
    rues_expanded = []
    for base_rue in rues_base:
        for i in range(50):  # 50 variantes par rue de base
            rues_expanded.append(f"{base_rue} {['Nord', 'Sud', 'Est', 'Ouest', 'Centre'][i % 5]} {i//5 + 1}")
    
    output_file = f"sources/population_{city.lower()}.csv"
    
    # Générer et sauvegarder par chunks
    chunks_generated = 0
    total_rows = 0
    
    for chunk_start in range(0, size, chunk_size):
        chunk_end = min(chunk_start + chunk_size, size)
        current_chunk_size = chunk_end - chunk_start
        
        chunk_data = []
        
        for i in range(current_chunk_size):
            global_id = id_offset + chunk_start + i + 1
            
            # Optimisation: utilisation de cycles pré-générés
            nom = random.choice(noms_cycle) if random.random() > 0.02 else None
            prenom = random.choice(prenoms_cycle) if random.random() > 0.015 else None
            
            # Adresse optimisée
            if random.random() > 0.01:
                numero = random.randint(1, 999)
                rue = random.choice(rues_expanded)
                adresse = f"{numero} {rue}"
            else:
                adresse = None
            
            # CSP avec distribution réaliste française
            csp_rand = random.random()
            if csp_rand < 0.03:  # 3% Agriculteurs
                csp = "1"
            elif csp_rand < 0.09:  # 6% Artisans/Commerçants
                csp = "2"
            elif csp_rand < 0.25:  # 16% Cadres
                csp = "3"
            elif csp_rand < 0.50:  # 25% Professions intermédiaires
                csp = "4"
            elif csp_rand < 0.78:  # 28% Employés
                csp = "5"
            elif csp_rand < 0.92:  # 14% Ouvriers
                csp = "6"
            elif csp_rand < 0.97:  # 5% Retraités
                csp = "7"
            else:  # 3% Sans activité
                csp = "8"
            
            # Problèmes de qualité intentionnels
            if random.random() < 0.15:  # 15% de codes problématiques
                problematic_csp = [
                    "cadre", "employé", "ouvrier", "retraité", "9", "0", "NC", "X", None
                ]
                csp = random.choice(problematic_csp)
            
            chunk_data.append({
                'ID_Personne': global_id,
                'Nom': nom,
                'Prenom': prenom,
                'Adresse': adresse,
                'CSP': csp
            })
        
        # Ajouter des doublons dans le chunk (3%)
        duplicates_count = int(current_chunk_size * 0.03)
        for _ in range(duplicates_count):
            if chunk_data:
                original = random.choice(chunk_data)
                duplicate = original.copy()
                # Variation mineure
                if random.random() < 0.5 and duplicate['Prenom']:
                    duplicate['Prenom'] = duplicate['Prenom'].upper()
                chunk_data.append(duplicate)
        
        # Convertir en DataFrame et sauvegarder
        chunk_df = pd.DataFrame(chunk_data)
        
        # Première fois: créer le fichier avec header
        if chunks_generated == 0:
            chunk_df.to_csv(output_file, index=False, mode='w')
        else:
            # Ajouter sans header
            chunk_df.to_csv(output_file, index=False, mode='a', header=False)
        
        total_rows += len(chunk_df)
        chunks_generated += 1
        
        progress = (chunk_end / size) * 100
        print(f"  ✓ Chunk {chunks_generated}: {chunk_end:,}/{size:,} ({progress:.1f}%) - {len(chunk_df):,} lignes")
        
        # Libérer la mémoire
        del chunk_data, chunk_df
    
    print(f"  ✅ Population {city} complète: {total_rows:,} lignes générées")
    return total_rows

def generate_massive_consumption(city, size, chunk_size=50000):
    """Générer des données de consommation massives."""
    
    print(f"⚡ Génération MASSIVE de {size:,} relevés de consommation pour {city}...")
    
    if city.lower() == "paris":
        codes_postaux = [75001, 75002, 75003, 75004, 75005, 75006, 75007, 75008, 75009, 75010,
                        75011, 75012, 75013, 75014, 75015, 75016, 75017, 75018, 75019, 75020]
        id_offset = 0
    else:
        codes_postaux = [91000, 91100, 91200, 91300, 91400, 91500]
        id_offset = 20_000_000
    
    # Rues pré-générées pour optimisation
    rues_expanded = [f"Rue de la {word} {i}" for word in ["Paix", "République", "Liberté", "Fraternité", "Justice"] for i in range(1, 201)]
    
    output_file = f"sources/consommation_{city.lower()}.csv"
    
    chunks_generated = 0
    total_rows = 0
    
    for chunk_start in range(0, size, chunk_size):
        chunk_end = min(chunk_start + chunk_size, size)
        current_chunk_size = chunk_end - chunk_start
        
        chunk_data = []
        
        for i in range(current_chunk_size):
            global_id = id_offset + chunk_start + i + 1
            
            # Numéro avec valeurs manquantes
            n = random.randint(1, 500) if random.random() > 0.02 else None
            
            # Rue aléatoire
            nom_rue = random.choice(rues_expanded)
            
            # Code postal
            code_postal = random.choice(codes_postaux)
            
            # Consommation avec distribution réaliste
            # Distribution log-normale pour avoir des valeurs plus réalistes
            base_consumption = np.random.lognormal(mean=2.8, sigma=0.5)  # ~16 kWh/jour en moyenne
            
            # Facteur saisonnier
            seasonal = 1 + 0.4 * np.sin(2 * np.pi * random.random())
            consumption = base_consumption * seasonal
            
            # Valeurs aberrantes (1%)
            if random.random() < 0.01:
                if random.random() < 0.5:
                    consumption = random.uniform(0.1, 3)  # Très faible
                else:
                    consumption = random.uniform(80, 200)  # Très élevée
            
            # Valeurs impossibles (0.3%)
            if random.random() < 0.003:
                consumption = -abs(consumption)
            
            # Valeurs manquantes (2.5%)
            if random.random() < 0.025:
                consumption = None
            else:
                consumption = round(consumption, 1)
            
            chunk_data.append({
                'ID_Adr': global_id,
                'N': n,
                'Nom_Rue': nom_rue,
                'Code_Postal': code_postal,
                'NB_KW_Jour': consumption
            })
        
        # Doublons (2%)
        duplicates_count = int(current_chunk_size * 0.02)
        for _ in range(duplicates_count):
            if chunk_data:
                chunk_data.append(random.choice(chunk_data).copy())
        
        # Sauvegarder le chunk
        chunk_df = pd.DataFrame(chunk_data)
        
        if chunks_generated == 0:
            chunk_df.to_csv(output_file, index=False, mode='w')
        else:
            chunk_df.to_csv(output_file, index=False, mode='a', header=False)
        
        total_rows += len(chunk_df)
        chunks_generated += 1
        
        progress = (chunk_end / size) * 100
        print(f"  ✓ Chunk {chunks_generated}: {chunk_end:,}/{size:,} ({progress:.1f}%) - {len(chunk_df):,} lignes")
        
        del chunk_data, chunk_df
    
    print(f"  ✅ Consommation {city} complète: {total_rows:,} lignes générées")
    return total_rows

def generate_massive_datasets(scale='medium'):
    """Générer des datasets massifs selon la configuration choisie."""
    
    if scale not in SCALE_CONFIGS:
        print(f"❌ Configuration '{scale}' inconnue. Configurations disponibles:")
        for key, config in SCALE_CONFIGS.items():
            print(f"  {key}: {config['description']}")
        return
    
    config = SCALE_CONFIGS[scale]
    
    print("🚀 GÉNÉRATION DE DATASETS MASSIFS")
    print("=" * 50)
    print(f"Configuration: {scale.upper()}")
    print(f"Description: {config['description']}")
    print(f"Population Paris: {config['population_paris']:,}")
    print(f"Population Évry: {config['population_evry']:,}")
    print(f"Consommation Paris: {config['consumption_paris']:,}")
    print(f"Consommation Évry: {config['consumption_evry']:,}")
    
    # Estimation de l'espace disque nécessaire
    estimated_size_mb = (
        config['population_paris'] * 0.045 +  # ~45 bytes par ligne population
        config['population_evry'] * 0.045 +
        config['consumption_paris'] * 0.040 +  # ~40 bytes par ligne consommation
        config['consumption_evry'] * 0.040
    )
    
    print(f"Espace disque estimé: ~{estimated_size_mb:.0f} MB")
    print()
    
    # Vérification de l'espace disque disponible
    disk_usage = os.statvfs('.') if hasattr(os, 'statvfs') else None
    if disk_usage:
        available_gb = (disk_usage.f_bavail * disk_usage.f_frsize) / (1024**3)
        if estimated_size_mb / 1024 > available_gb * 0.8:
            print(f"⚠️  Attention: Espace disque possiblement insuffisant ({available_gb:.1f} GB disponibles)")
    
    confirm = input("Continuer la génération? (o/N): ").lower().strip()
    if confirm not in ['o', 'oui', 'y', 'yes']:
        print("❌ Génération annulée")
        return
    
    start_time = datetime.now()
    
    # Créer le dossier sources
    Path("sources").mkdir(exist_ok=True)
    
    # Génération avec chunks pour économiser la mémoire
    chunk_size = min(50000, config['population_paris'] // 20)  # Chunks adaptatifs
    
    print(f"📊 Taille des chunks: {chunk_size:,} lignes")
    print()
    
    total_rows = 0
    
    # 1. Population Paris
    rows = generate_massive_population("Paris", config['population_paris'], chunk_size)
    total_rows += rows
    
    # 2. Population Évry
    rows = generate_massive_population("Évry", config['population_evry'], chunk_size)
    total_rows += rows
    
    # 3. Consommation Paris
    rows = generate_massive_consumption("Paris", config['consumption_paris'], chunk_size)
    total_rows += rows
    
    # 4. Consommation Évry
    rows = generate_massive_consumption("Évry", config['consumption_evry'], chunk_size)
    total_rows += rows
    
    # 5. Tables de référence (pas besoin de chunks)
    print("👥 Génération CSP et IRIS...")
    
    # CSP (même table que précédemment)
    csp_data = [
        {"ID_CSP": i+1, "Desc": desc, "Salaire_Moyen": sal, "Salaire_Min": sal_min, "Salaire_Max": sal_max}
        for i, (desc, sal, sal_min, sal_max) in enumerate([
            ("Agriculteurs exploitants", 25000, 15000, 45000),
            ("Artisans, commerçants et chefs d'entreprise", 45000, 20000, 120000),
            ("Cadres et professions intellectuelles supérieures", 65000, 40000, 150000),
            ("Professions intermédiaires", 35000, 25000, 50000),
            ("Employés", 28000, 20000, 40000),
            ("Ouvriers", 25000, 18000, 35000),
            ("Retraités", 18000, 8000, 35000),
            ("Autres personnes sans activité professionnelle", 12000, 0, 25000)
        ])
    ]
    pd.DataFrame(csp_data).to_csv("sources/csp.csv", index=False)
    
    # IRIS étendu
    iris_data = []
    # Paris: 20 arrondissements × 50 IRIS = 1000 IRIS
    for arr in range(1, 21):
        for iris_num in range(1, 51):
            iris_data.append({
                'ID_Rue': (arr-1)*50 + iris_num,
                'ID_Ville': 'Paris',
                'ID_IRIS': f'IRIS_750{arr:02d}_{iris_num:03d}'
            })
    
    # Évry: 200 IRIS
    for iris_num in range(1, 201):
        iris_data.append({
            'ID_Rue': 10000 + iris_num,
            'ID_Ville': 'Evry',
            'ID_IRIS': f'IRIS_91000_{iris_num:03d}'
        })
    
    pd.DataFrame(iris_data).to_csv("sources/iris.csv", index=False)
    total_rows += len(csp_data) + len(iris_data)
    
    print(f"  ✅ {len(csp_data)} CSP + {len(iris_data)} IRIS générés")
    
    # Statistiques finales
    end_time = datetime.now()
    duration = end_time - start_time
    
    print()
    print("📊 STATISTIQUES FINALES")
    print("-" * 40)
    
    # Tailles des fichiers
    for filename in ['population_paris.csv', 'population_evry.csv', 
                    'consommation_paris.csv', 'consommation_evry.csv',
                    'csp.csv', 'iris.csv']:
        filepath = Path(f"sources/{filename}")
        if filepath.exists():
            size_mb = filepath.stat().st_size / (1024 * 1024)
            rows = sum(1 for _ in open(filepath)) - 1  # -1 pour le header
            print(f"{filename:25} : {size_mb:8.1f} MB ({rows:,} lignes)")
    
    total_size_mb = sum(f.stat().st_size for f in Path("sources").glob("*.csv")) / (1024 * 1024)
    
    print("-" * 40)
    print(f"{'TOTAL':25} : {total_size_mb:8.1f} MB ({total_rows:,} lignes)")
    print(f"Durée de génération: {duration}")
    print(f"Vitesse: {total_rows / duration.total_seconds():.0f} lignes/seconde")
    print()
    print("✅ Génération massive terminée!")
    print("🚀 Prêt pour des tests ETL haute performance!")

def main():
    """Point d'entrée principal avec interface utilisateur."""
    
    print("🎯 GÉNÉRATEUR DE DATASETS MASSIFS")
    print("Choisissez la taille des données à générer:")
    print()
    
    for key, config in SCALE_CONFIGS.items():
        print(f"  {key:8} : {config['description']}")
    
    print()
    
    if len(sys.argv) > 1:
        scale = sys.argv[1].lower()
    else:
        scale = input("Entrez la configuration souhaitée (small/medium/large/xlarge/xxlarge): ").lower().strip()
    
    if not scale:
        scale = 'small'
        print(f"Configuration par défaut: {scale}")
    
    generate_massive_datasets(scale)

if __name__ == "__main__":
    main()