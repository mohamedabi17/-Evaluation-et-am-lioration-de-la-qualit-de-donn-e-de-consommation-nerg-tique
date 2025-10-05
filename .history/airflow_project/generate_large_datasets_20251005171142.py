#!/usr/bin/env python3
"""
Générateur de données synthétiques en grande quantité pour le projet ETL
Ce script génère des datasets volumineux avec des problèmes de qualité réalistes
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

# Configuration
POPULATION_SIZE_PARIS = 50000
POPULATION_SIZE_EVRY = 25000
CONSUMPTION_RECORDS_PARIS = 75000
CONSUMPTION_RECORDS_EVRY = 35000
CSP_CATEGORIES = 8
IRIS_ZONES_PARIS = 150
IRIS_ZONES_EVRY = 80

# Données de référence françaises
NOMS_FRANCAIS = [
    "Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand",
    "Leroy", "Moreau", "Simon", "Laurent", "Lefebvre", "Michel", "Garcia", "David",
    "Bertrand", "Roux", "Vincent", "Fournier", "Morel", "Girard", "Andre", "Lefevre",
    "Mercier", "Dupont", "Lambert", "Bonnet", "Francois", "Martinez", "Legrand", "Garnier",
    "Faure", "Rousseau", "Blanc", "Guerin", "Muller", "Henry", "Roussel", "Nicolas",
    "Perrin", "Morin", "Mathieu", "Clement", "Gauthier", "Dumont", "Lopez", "Fontaine",
    "Chevalier", "Robin", "Masson", "Sanchez", "Gerard", "Nguyen", "Boyer", "Denis",
    "Lemaire", "Duval", "Joly", "Gautier", "Roger", "Roche", "Roy", "Noel"
]

PRENOMS_FRANCAIS = [
    "Jean", "Marie", "Michel", "Alain", "Patrick", "Pierre", "Philippe", "Christophe",
    "Daniel", "Bernard", "Nicolas", "Laurent", "Thierry", "Eric", "Frederic", "David",
    "Stephane", "Pascal", "Olivier", "Julien", "Sebastien", "Fabrice", "Bruno", "Vincent",
    "Sylvie", "Isabelle", "Catherine", "Francoise", "Monique", "Nathalie", "Valerie", "Sandrine",
    "Martine", "Nicole", "Veronique", "Christine", "Brigitte", "Dominique", "Chantal", "Jacqueline",
    "Anne", "Francine", "Karine", "Corinne", "Pascale", "Aurelie", "Celine", "Emilie",
    "Julie", "Laetitia", "Stephanie", "Sophie", "Delphine", "Caroline", "Virginie", "Marianne",
    "Claire", "Emma", "Camille", "Lea", "Manon", "Lisa", "Sarah", "Oceane"
]

RUES_PARIS = [
    "Rue de Rivoli", "Avenue des Champs-Elysées", "Boulevard Saint-Germain", "Rue de la Paix",
    "Avenue Montaigne", "Rue du Faubourg Saint-Honoré", "Boulevard Haussmann", "Rue Lafayette",
    "Avenue Victor Hugo", "Rue Saint-Antoine", "Boulevard Voltaire", "Rue de Belleville",
    "Avenue de la République", "Rue Oberkampf", "Boulevard de Magenta", "Rue de Charonne",
    "Avenue Parmentier", "Rue du Temple", "Boulevard Beaumarchais", "Rue de Ménilmontant",
    "Avenue Ledru-Rollin", "Rue de la Roquette", "Boulevard Richard-Lenoir", "Rue Saint-Maur",
    "Avenue Philippe-Auguste", "Rue de la Folie-Méricourt", "Boulevard Jules Ferry", "Rue Amelot",
    "Avenue de la Bastille", "Rue Popincourt", "Boulevard du Temple", "Rue Jean-Pierre Timbaud",
    "Avenue Trudaine", "Rue des Martyrs", "Boulevard de Rochechouart", "Rue Condorcet",
    "Avenue de Clichy", "Rue Blanche", "Boulevard des Batignolles", "Rue de Rome",
    "Avenue de Wagram", "Rue de Lévis", "Boulevard Malesherbes", "Rue de Courcelles",
    "Avenue Hoche", "Rue de Prony", "Boulevard Pereire", "Rue Cardinet"
]

RUES_EVRY = [
    "Rue de l'Essonne", "Avenue de la Gare", "Boulevard des Champs", "Rue de la République",
    "Avenue du Parc", "Boulevard Liberté", "Avenue Jean Jaurès", "Rue Nationale",
    "Avenue François Mitterrand", "Rue du Commerce", "Boulevard de la Paix", "Avenue de l'Europe",
    "Rue des Écoles", "Avenue du Général de Gaulle", "Boulevard Victor Hugo", "Rue Pasteur",
    "Avenue Carnot", "Rue Gambetta", "Boulevard Clemenceau", "Avenue Foch",
    "Rue de la Mairie", "Avenue des Tilleuls", "Boulevard des Platanes", "Rue des Roses",
    "Avenue des Lilas", "Rue du Stade", "Boulevard des Sports", "Avenue de la Poste",
    "Rue de l'Église", "Avenue des Écoles", "Boulevard de l'Hôpital", "Rue de la Gendarmerie"
]

CODES_POSTAUX_PARIS = [
    75001, 75002, 75003, 75004, 75005, 75006, 75007, 75008, 75009, 75010,
    75011, 75012, 75013, 75014, 75015, 75016, 75017, 75018, 75019, 75020
]

CSP_DESCRIPTIONS = [
    "Agriculteurs exploitants",
    "Artisans, commerçants et chefs d'entreprise", 
    "Cadres et professions intellectuelles supérieures",
    "Professions intermédiaires",
    "Employés",
    "Ouvriers",
    "Retraités",
    "Autres personnes sans activité professionnelle"
]

def generate_population_data(city, size):
    """Générer des données de population avec des problèmes de qualité."""
    
    print(f"📊 Génération de {size:,} habitants pour {city}...")
    
    data = []
    
    for i in range(1, size + 1):
        # ID séquentiel avec offset par ville
        if city.lower() == "paris":
            id_personne = i
        else:
            id_personne = 100000 + i
        
        # Noms avec quelques valeurs manquantes (2%)
        nom = random.choice(NOMS_FRANCAIS) if random.random() > 0.02 else None
        
        # Prénoms avec quelques valeurs manquantes (1.5%)
        prenom = random.choice(PRENOMS_FRANCAIS) if random.random() > 0.015 else None
        
        # Adresses
        if city.lower() == "paris":
            rue = random.choice(RUES_PARIS)
            numero = random.randint(1, 200)
        else:
            rue = random.choice(RUES_EVRY)
            numero = random.randint(1, 150)
        
        # Adresse complète avec quelques valeurs manquantes (1%)
        if random.random() > 0.01:
            adresse = f"{numero} {rue}"
        else:
            adresse = None
        
        # CSP avec différents formats pour créer des problèmes de codification
        csp_choice = random.random()
        if csp_choice < 0.7:  # 70% codes numériques corrects
            csp = str(random.randint(1, 8))
        elif csp_choice < 0.85:  # 15% codes textuels
            csp_mapping = {
                1: "cadre", 2: "employé", 3: "ouvrier", 4: "retraité",
                5: "artisan", 6: "profession_intermediaire", 7: "agriculteur", 8: "sans_activite"
            }
            csp = csp_mapping[random.randint(1, 8)]
        elif csp_choice < 0.95:  # 10% codes invalides
            invalid_codes = ["9", "X", "NC", "0", "99", "cadres", "employes"]
            csp = random.choice(invalid_codes)
        else:  # 5% valeurs manquantes
            csp = None
        
        data.append({
            'ID_Personne': id_personne,
            'Nom': nom,
            'Prenom': prenom,
            'Adresse': adresse,
            'CSP': csp
        })
        
        # Progress indicator
        if i % 10000 == 0:
            print(f"  ✓ {i:,} habitants générés...")
    
    # Ajouter des doublons intentionnels (3%)
    duplicates_count = int(size * 0.03)
    print(f"  🔄 Ajout de {duplicates_count:,} doublons...")
    
    for _ in range(duplicates_count):
        original_row = random.choice(data)
        duplicate = original_row.copy()
        # Petites variations pour simuler des doublons "presque" identiques
        if random.random() < 0.3:
            duplicate['Prenom'] = duplicate['Prenom'].upper() if duplicate['Prenom'] else None
        data.append(duplicate)
    
    df = pd.DataFrame(data)
    print(f"  ✅ Dataset final: {len(df):,} lignes")
    return df

def generate_consumption_data(city, size, population_ids):
    """Générer des données de consommation énergétique."""
    
    print(f"⚡ Génération de {size:,} relevés de consommation pour {city}...")
    
    data = []
    
    # Créer une correspondance adresse -> ID pour cohérence
    if city.lower() == "paris":
        rues_list = RUES_PARIS
        codes_postaux = CODES_POSTAUX_PARIS
        id_offset = 0
    else:
        rues_list = RUES_EVRY
        codes_postaux = [91000]
        id_offset = 100000
    
    for i in range(1, size + 1):
        id_adr = id_offset + i
        
        # Numéro de rue avec quelques valeurs manquantes (2%)
        n = random.randint(1, 250) if random.random() > 0.02 else None
        
        # Nom de rue
        nom_rue = random.choice(rues_list)
        
        # Code postal
        code_postal = random.choice(codes_postaux)
        
        # Consommation avec distribution réaliste et valeurs aberrantes
        base_consumption = np.random.normal(18, 5)  # Moyenne 18 kWh/jour, écart-type 5
        
        # Ajouter de la variabilité saisonnière
        seasonal_factor = 1 + 0.3 * np.sin(2 * np.pi * random.random())
        consumption = base_consumption * seasonal_factor
        
        # Valeurs aberrantes (1%)
        if random.random() < 0.01:
            consumption = random.choice([random.uniform(0, 2), random.uniform(80, 150)])
        
        # Valeurs négatives impossibles (0.5%)
        if random.random() < 0.005:
            consumption = -abs(consumption)
        
        # Valeurs manquantes (2.5%)
        if random.random() < 0.025:
            consumption = None
        else:
            consumption = round(consumption, 1)
        
        data.append({
            'ID_Adr': id_adr,
            'N': n,
            'Nom_Rue': nom_rue,
            'Code_Postal': code_postal,
            'NB_KW_Jour': consumption
        })
        
        if i % 15000 == 0:
            print(f"  ✓ {i:,} relevés générés...")
    
    # Ajouter des doublons (2%)
    duplicates_count = int(size * 0.02)
    print(f"  🔄 Ajout de {duplicates_count:,} doublons...")
    
    for _ in range(duplicates_count):
        original_row = random.choice(data)
        data.append(original_row.copy())
    
    df = pd.DataFrame(data)
    print(f"  ✅ Dataset final: {len(df):,} lignes")
    return df

def generate_csp_data():
    """Générer la table des catégories socio-professionnelles."""
    
    print("👥 Génération des catégories socio-professionnelles...")
    
    # Données réalistes françaises
    csp_data = [
        {"ID_CSP": 1, "Desc": "Agriculteurs exploitants", "Salaire_Moyen": 25000, "Salaire_Min": 15000, "Salaire_Max": 45000},
        {"ID_CSP": 2, "Desc": "Artisans, commerçants et chefs d'entreprise", "Salaire_Moyen": 45000, "Salaire_Min": 20000, "Salaire_Max": 120000},
        {"ID_CSP": 3, "Desc": "Cadres et professions intellectuelles supérieures", "Salaire_Moyen": 65000, "Salaire_Min": 40000, "Salaire_Max": 150000},
        {"ID_CSP": 4, "Desc": "Professions intermédiaires", "Salaire_Moyen": 35000, "Salaire_Min": 25000, "Salaire_Max": 50000},
        {"ID_CSP": 5, "Desc": "Employés", "Salaire_Moyen": 28000, "Salaire_Min": 20000, "Salaire_Max": 40000},
        {"ID_CSP": 6, "Desc": "Ouvriers", "Salaire_Moyen": 25000, "Salaire_Min": 18000, "Salaire_Max": 35000},
        {"ID_CSP": 7, "Desc": "Retraités", "Salaire_Moyen": 18000, "Salaire_Min": 8000, "Salaire_Max": 35000},
        {"ID_CSP": 8, "Desc": "Autres personnes sans activité professionnelle", "Salaire_Moyen": 12000, "Salaire_Min": 0, "Salaire_Max": 25000}
    ]
    
    df = pd.DataFrame(csp_data)
    print(f"  ✅ {len(df)} catégories CSP générées")
    return df

def generate_iris_data():
    """Générer la table IRIS (zones géographiques)."""
    
    print("🗺️  Génération des zones IRIS...")
    
    data = []
    
    # IRIS Paris
    for i in range(1, IRIS_ZONES_PARIS + 1):
        arrondissement = random.choice(CODES_POSTAUX_PARIS)
        data.append({
            'ID_Rue': i,
            'ID_Ville': 'Paris',
            'ID_IRIS': f'IRIS_{arrondissement}_{i:03d}'
        })
    
    # IRIS Évry
    for i in range(1, IRIS_ZONES_EVRY + 1):
        data.append({
            'ID_Rue': 100000 + i,
            'ID_Ville': 'Evry',
            'ID_IRIS': f'IRIS_91000_{i:03d}'
        })
    
    df = pd.DataFrame(data)
    print(f"  ✅ {len(df)} zones IRIS générées ({IRIS_ZONES_PARIS} Paris + {IRIS_ZONES_EVRY} Évry)")
    return df

def generate_all_datasets():
    """Générer tous les datasets en grande quantité."""
    
    print("🚀 GÉNÉRATION DE DONNÉES SYNTHÉTIQUES EN GRANDE QUANTITÉ")
    print("=" * 60)
    print(f"Configuration:")
    print(f"  - Population Paris: {POPULATION_SIZE_PARIS:,} habitants")
    print(f"  - Population Évry: {POPULATION_SIZE_EVRY:,} habitants") 
    print(f"  - Consommation Paris: {CONSUMPTION_RECORDS_PARIS:,} relevés")
    print(f"  - Consommation Évry: {CONSUMPTION_RECORDS_EVRY:,} relevés")
    print(f"  - Zones IRIS: {IRIS_ZONES_PARIS + IRIS_ZONES_EVRY} total")
    print()
    
    # Créer le dossier sources s'il n'existe pas
    sources_dir = "sources"
    if not os.path.exists(sources_dir):
        os.makedirs(sources_dir)
        print(f"📁 Dossier {sources_dir} créé")
    
    # 1. Population Paris
    pop_paris = generate_population_data("Paris", POPULATION_SIZE_PARIS)
    pop_paris.to_csv(f"{sources_dir}/population_paris.csv", index=False)
    print(f"💾 Sauvegardé: {sources_dir}/population_paris.csv")
    print()
    
    # 2. Population Évry
    pop_evry = generate_population_data("Évry", POPULATION_SIZE_EVRY)
    pop_evry.to_csv(f"{sources_dir}/population_evry.csv", index=False)
    print(f"💾 Sauvegardé: {sources_dir}/population_evry.csv")
    print()
    
    # 3. Consommation Paris
    conso_paris = generate_consumption_data("Paris", CONSUMPTION_RECORDS_PARIS, pop_paris['ID_Personne'].tolist())
    conso_paris.to_csv(f"{sources_dir}/consommation_paris.csv", index=False)
    print(f"💾 Sauvegardé: {sources_dir}/consommation_paris.csv")
    print()
    
    # 4. Consommation Évry
    conso_evry = generate_consumption_data("Évry", CONSUMPTION_RECORDS_EVRY, pop_evry['ID_Personne'].tolist())
    conso_evry.to_csv(f"{sources_dir}/consommation_evry.csv", index=False)
    print(f"💾 Sauvegardé: {sources_dir}/consommation_evry.csv")
    print()
    
    # 5. CSP
    csp = generate_csp_data()
    csp.to_csv(f"{sources_dir}/csp.csv", index=False)
    print(f"💾 Sauvegardé: {sources_dir}/csp.csv")
    print()
    
    # 6. IRIS
    iris = generate_iris_data()
    iris.to_csv(f"{sources_dir}/iris.csv", index=False)
    print(f"💾 Sauvegardé: {sources_dir}/iris.csv")
    print()
    
    # Statistiques finales
    total_rows = len(pop_paris) + len(pop_evry) + len(conso_paris) + len(conso_evry) + len(csp) + len(iris)
    print("📊 RÉSUMÉ DE LA GÉNÉRATION")
    print("-" * 40)
    print(f"Population Paris:     {len(pop_paris):8,} lignes")
    print(f"Population Évry:      {len(pop_evry):8,} lignes")
    print(f"Consommation Paris:   {len(conso_paris):8,} lignes")
    print(f"Consommation Évry:    {len(conso_evry):8,} lignes")
    print(f"CSP:                  {len(csp):8,} lignes")
    print(f"IRIS:                 {len(iris):8,} lignes")
    print("-" * 40)
    print(f"TOTAL:                {total_rows:8,} lignes")
    print()
    
    # Calculer la taille des fichiers
    total_size_mb = 0
    for filename in os.listdir(sources_dir):
        if filename.endswith('.csv'):
            filepath = os.path.join(sources_dir, filename)
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            total_size_mb += size_mb
            print(f"{filename:25} : {size_mb:6.1f} MB")
    
    print(f"{'TAILLE TOTALE':25} : {total_size_mb:6.1f} MB")
    print()
    print("✅ Génération terminée avec succès!")
    print("🔍 Les données incluent des problèmes de qualité intentionnels:")
    print("   - Valeurs manquantes (1-3%)")
    print("   - Doublons (2-3%)")
    print("   - Formats incohérents (codes CSP)")
    print("   - Valeurs aberrantes (consommation)")
    print("   - Codifications mixtes (texte/numérique)")

if __name__ == "__main__":
    # Permettre la personnalisation via arguments ou variables d'environnement
    import sys
    
    if len(sys.argv) > 1:
        try:
            scale_factor = float(sys.argv[1])
            POPULATION_SIZE_PARIS = int(POPULATION_SIZE_PARIS * scale_factor)
            POPULATION_SIZE_EVRY = int(POPULATION_SIZE_EVRY * scale_factor)
            CONSUMPTION_RECORDS_PARIS = int(CONSUMPTION_RECORDS_PARIS * scale_factor)
            CONSUMPTION_RECORDS_EVRY = int(CONSUMPTION_RECORDS_EVRY * scale_factor)
            print(f"🔧 Facteur d'échelle appliqué: {scale_factor}x")
        except ValueError:
            print("⚠️  Facteur d'échelle invalide, utilisation des valeurs par défaut")
    
    generate_all_datasets()