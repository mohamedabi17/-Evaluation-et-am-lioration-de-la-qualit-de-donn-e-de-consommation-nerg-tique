#!/usr/bin/env python3
"""
ETL Local Runner - Exécute le pipeline ETL sans Docker
Simule le comportement du DAG Airflow en local pour demonstration
"""

import os
import sys
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import time

# Ajouter le répertoire utils au path
sys.path.append('utils')

from quality_checks import (
    generate_quality_report, 
    create_quality_visualizations,
    check_completeness,
    check_duplicates,
    check_codification_consistency
)

from data_quality_improver import (
    DataQualityImprover,
    create_improvement_config_population,
    create_improvement_config_consommation
)

# Configuration des chemins (local)
SOURCES_PATH = "sources"
OUTPUT_PATH = "output"
QUALITY_REPORTS_PATH = "quality_reports"
VISUALIZATIONS_PATH = "visualizations"

class LocalETLRunner:
    """Classe pour exécuter l'ETL en local."""
    
    def __init__(self):
        self.create_directories()
        self.execution_log = []
        self.start_time = datetime.now()
    
    def create_directories(self):
        """Créer les dossiers de sortie."""
        for path in [OUTPUT_PATH, QUALITY_REPORTS_PATH, VISUALIZATIONS_PATH]:
            Path(path).mkdir(exist_ok=True)
        print("📁 Dossiers de sortie créés")
    
    def log_step(self, step_name, details=None):
        """Logger une étape de l'ETL."""
        timestamp = datetime.now()
        log_entry = {
            'step': step_name,
            'timestamp': timestamp.isoformat(),
            'details': details
        }
        self.execution_log.append(log_entry)
        print(f"✅ {step_name} - {timestamp.strftime('%H:%M:%S')}")
        if details:
            print(f"   {details}")
    
    def extract_source_data(self, source_name):
        """Extraire les données sources."""
        file_path = f"{SOURCES_PATH}/{source_name}.csv"
        
        if not os.path.exists(file_path):
            print(f"❌ Fichier {file_path} non trouvé")
            return None
        
        df = pd.read_csv(file_path)
        
        stats = {
            'rows': len(df),
            'columns': len(df.columns),
            'file_size_kb': round(os.path.getsize(file_path) / 1024, 2)
        }
        
        self.log_step(f"Extraction {source_name}", 
                     f"{stats['rows']:,} lignes, {stats['columns']} colonnes, {stats['file_size_kb']} KB")
        
        return df, stats
    
    def evaluate_data_quality(self, source_name, df):
        """Évaluer la qualité des données."""
        print(f"\n🔍 Évaluation qualité: {source_name}")
        
        # Générer le rapport de qualité
        report_path = f"{QUALITY_REPORTS_PATH}/{source_name}_quality_report.json"
        quality_report = generate_quality_report(df, source_name, report_path)
        
        # Créer les visualisations
        try:
            create_quality_visualizations(quality_report, VISUALIZATIONS_PATH)
        except Exception as e:
            print(f"⚠️  Erreur visualisation: {e}")
        
        # Log des métriques importantes
        quality_score = quality_report['quality_score']
        
        details = (f"Score: {quality_score['overall_quality_score']}% ({quality_score['quality_level']}) | "
                  f"Complétude: {quality_score['completeness_score']}% | "
                  f"Unicité: {quality_score['uniqueness_score']}%")
        
        self.log_step(f"Qualité évaluée {source_name}", details)
        
        return quality_report
    
    def decide_cleaning_strategy(self, quality_reports):
        """Décider de la stratégie de nettoyage."""
        print(f"\n🤔 Décision stratégie de nettoyage...")
        
        needs_cleaning = False
        quality_threshold = 99  # Seuil d'acceptabilité à 99%
        
        for source_name, report in quality_reports.items():
            score = report['quality_score']['overall_quality_score']
            if score < quality_threshold:
                needs_cleaning = True
                print(f"   {source_name}: Score {score:.2f}% < {quality_threshold}% - Nettoyage requis")
            else:
                print(f"   {source_name}: Score {score:.2f}% ≥ {quality_threshold}% - Qualité excellente")
        
        if needs_cleaning:
            self.log_step("Stratégie", f"Qualité < {quality_threshold}% - Amélioration activée")
            return 'improve'
        else:
            self.log_step("Stratégie", f"Qualité ≥ {quality_threshold}% - Intégration directe")
            return 'integrate'
    
    def improve_data_quality(self, datasets):
        """Améliorer la qualité des données."""
        print(f"\n🧹 Amélioration de la qualité des données...")
        
        improved_datasets = {}
        
        for source_name, df in datasets.items():
            print(f"\n   🔧 Traitement de {source_name}...")
            print(f"      📊 Données initiales: {len(df):,} lignes")
            
            improver = DataQualityImprover()
            
            # Sélectionner la configuration appropriée
            if 'population' in source_name:
                config = create_improvement_config_population()
            else:
                config = create_improvement_config_consommation()
            
            # Appliquer les améliorations étape par étape
            print(f"      🧹 Suppression des doublons...")
            df_improved = improver.remove_duplicates(df)
            print(f"         ✅ {len(df) - len(df_improved)} doublons supprimés")
            
            print(f"      📝 Amélioration de la complétude...")
            df_improved = improver.improve_completeness(df_improved, config['completeness_strategies'])
            
            print(f"      🎨 Standardisation des formats...")
            df_improved = improver.standardize_format(df_improved, config['format_rules'])
            
            # Appliquer la codification seulement si elle existe
            if 'codification_rules' in config:
                print(f"      🔢 Normalisation de la codification...")
                df_improved = improver.normalize_codification(df_improved, config['codification_rules'])
            
            print(f"      📋 Application des règles métier...")
            df_improved = improver.apply_business_rules(df_improved, config['business_rules'])
            
            # Sauvegarder les données améliorées
            output_file = f"{OUTPUT_PATH}/{source_name}_clean.csv"
            df_improved.to_csv(output_file, index=False)
            
            improved_datasets[source_name] = df_improved
            
            # Afficher le résumé détaillé des améliorations
            improvement_summary = improver.get_improvement_summary()
            total_actions = sum(improvement_summary['by_category'].values())
            
            print(f"      📊 Résultat: {len(df_improved):,} lignes finales")
            print(f"      ✅ {total_actions} actions d'amélioration appliquées:")
            
            for category, count in improvement_summary['by_category'].items():
                print(f"         • {category}: {count} action(s)")
            
            self.log_step(f"Amélioration {source_name}", 
                         f"{total_actions} actions appliquées - {len(df_improved):,} lignes finales")
        
        return improved_datasets
    
    def integrate_data(self, datasets):
        """Intégrer les données pour créer les tables cibles."""
        print(f"\n🔗 Intégration des données...")
        
        # Charger les données de référence
        csp = pd.read_csv(f"{SOURCES_PATH}/csp.csv")
        iris = pd.read_csv(f"{SOURCES_PATH}/iris.csv")
        
        results = {}
        
        # 1. Créer Consommation_IRIS_Paris
        if 'population_paris' in datasets and 'consommation_paris' in datasets:
            result_paris = self.create_consommation_iris_table(
                datasets['population_paris'], 
                datasets['consommation_paris'], 
                iris, 
                "paris"
            )
            results['consommation_iris_paris'] = result_paris
        
        # 2. Créer Consommation_IRIS_Evry
        if 'population_evry' in datasets and 'consommation_evry' in datasets:
            result_evry = self.create_consommation_iris_table(
                datasets['population_evry'], 
                datasets['consommation_evry'], 
                iris, 
                "evry"
            )
            results['consommation_iris_evry'] = result_evry
        
        # 3. Créer Consommation_CSP
        if all(k in datasets for k in ['population_paris', 'population_evry', 'consommation_paris', 'consommation_evry']):
            result_csp = self.create_consommation_csp_table(
                datasets['population_paris'], 
                datasets['population_evry'],
                datasets['consommation_paris'], 
                datasets['consommation_evry'], 
                csp
            )
            results['consommation_csp'] = result_csp
        
        self.log_step("Intégration", f"{len(results)} tables cibles créées")
        return results
    
    def create_consommation_iris_table(self, population, consommation, iris, city):
        """Créer la table Consommation_IRIS pour une ville."""
        
        # Jointure Population + Consommation (simplifiée sur ID)
        merged = pd.merge(
            population[['ID_Personne', 'Adresse']], 
            consommation[['ID_Adr', 'Nom_Rue', 'NB_KW_Jour']], 
            left_on='ID_Personne', 
            right_on='ID_Adr', 
            how='inner'
        )
        
        # Jointure avec IRIS
        merged = pd.merge(
            merged,
            iris[iris['ID_Ville'].str.lower() == city.lower()],
            left_on='ID_Adr',
            right_on='ID_Rue',
            how='inner'
        )
        
        # Agrégation par IRIS
        result = merged.groupby('ID_IRIS')['NB_KW_Jour'].agg(['mean', 'count']).reset_index()
        result.columns = ['ID_IRIS', 'Conso_moyenne_annuelle', 'Nb_logements']
        
        # Conversion en consommation annuelle
        result['Conso_moyenne_annuelle'] = (result['Conso_moyenne_annuelle'] * 365).round(2)
        
        # Sauvegarder
        output_file = f"{OUTPUT_PATH}/consommation_iris_{city}.csv"
        result.to_csv(output_file, index=False)
        
        self.log_step(f"Table IRIS {city.title()}", f"{len(result)} zones IRIS")
        
        return result
    
    def create_consommation_csp_table(self, pop_paris, pop_evry, conso_paris, conso_evry, csp):
        """Créer la table Consommation_CSP globale."""
        
        # Combiner les données
        pop_all = pd.concat([pop_paris, pop_evry])
        conso_all = pd.concat([conso_paris, conso_evry])
        
        # Standardiser le type de la colonne CSP (convertir en string)
        pop_all['CSP'] = pop_all['CSP'].astype(str)
        csp['ID_CSP'] = csp['ID_CSP'].astype(str)
        
        # Jointure Population + Consommation
        merged = pd.merge(
            pop_all[['ID_Personne', 'CSP']], 
            conso_all[['ID_Adr', 'NB_KW_Jour']], 
            left_on='ID_Personne', 
            right_on='ID_Adr', 
            how='inner'
        )
        
        # Agrégation par CSP
        csp_consumption = merged.groupby('CSP')['NB_KW_Jour'].agg(['mean', 'count']).reset_index()
        csp_consumption.columns = ['CSP', 'Conso_moyenne_annuelle', 'Nb_personnes']
        
        # S'assurer que les types sont compatibles pour la jointure
        csp_consumption['CSP'] = csp_consumption['CSP'].astype(str)
        
        # Jointure avec la table CSP
        result = pd.merge(
            csp_consumption,
            csp[['ID_CSP', 'Desc', 'Salaire_Moyen']],
            left_on='CSP',
            right_on='ID_CSP',
            how='left'
        )
        
        # Conversion en consommation annuelle
        result['Conso_moyenne_annuelle'] = (result['Conso_moyenne_annuelle'] * 365).round(2)
        
        # Sélection des colonnes finales
        result = result[['ID_CSP', 'Desc', 'Conso_moyenne_annuelle', 'Salaire_Moyen', 'Nb_personnes']]
        
        # Sauvegarder
        output_file = f"{OUTPUT_PATH}/consommation_csp.csv"
        result.to_csv(output_file, index=False)
        
        self.log_step("Table CSP", f"{len(result)} catégories socio-professionnelles")
        
        return result
    
    def generate_final_report(self, target_tables):
        """Générer un rapport final de l'ETL."""
        
        print(f"\n📊 Génération du rapport final...")
        
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        final_report = {
            'etl_execution': {
                'start_time': self.start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration.total_seconds(),
                'status': 'completed'
            },
            'target_tables': {},
            'execution_log': self.execution_log
        }
        
        # Statistiques des tables cibles
        for table_name, df in target_tables.items():
            if df is not None and len(df) > 0:
                final_report['target_tables'][table_name] = {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'file': f"{OUTPUT_PATH}/{table_name}.csv"
                }
        
        # Sauvegarder le rapport
        report_file = f"{QUALITY_REPORTS_PATH}/final_etl_report.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(final_report, f, indent=2, ensure_ascii=False)
        
        self.log_step("Rapport final", f"ETL terminé en {duration.total_seconds():.1f}s")
        
        return final_report

def run_local_etl():
    """Fonction principale pour exécuter l'ETL en local."""
    
    print("🚀 LANCEMENT DE L'ETL LOCAL")
    print("=" * 50)
    print("Simulation du DAG Airflow 'energy_data_quality_etl' en local")
    print("Traitement des données sans conteneurs Docker")
    print()
    
    # Vérifier la présence des données sources
    sources_dir = Path(SOURCES_PATH)
    if not sources_dir.exists():
        print(f"❌ Dossier '{SOURCES_PATH}' non trouvé!")
        return
    
    required_files = ['population_paris.csv', 'population_evry.csv', 
                     'consommation_paris.csv', 'consommation_evry.csv',
                     'csp.csv', 'iris.csv']
    
    missing_files = [f for f in required_files if not (sources_dir / f).exists()]
    if missing_files:
        print(f"❌ Fichiers manquants: {', '.join(missing_files)}")
        print("Générez d'abord les données avec generate_large_datasets.py")
        return
    
    # Initialiser l'ETL
    etl = LocalETLRunner()
    
    try:
        # ÉTAPE 1: Extraction
        print("\n📥 ÉTAPE 1: EXTRACTION DES DONNÉES")
        print("-" * 40)
        
        datasets = {}
        extraction_stats = {}
        
        sources = ['population_paris', 'population_evry', 'consommation_paris', 'consommation_evry']
        
        for source in sources:
            result = etl.extract_source_data(source)
            if result:
                datasets[source] = result[0]  # DataFrame
                extraction_stats[source] = result[1]  # Stats
        
        # ÉTAPE 2: Évaluation de la qualité
        print("\n🔍 ÉTAPE 2: ÉVALUATION DE LA QUALITÉ")
        print("-" * 40)
        
        quality_reports = {}
        for source_name, df in datasets.items():
            quality_reports[source_name] = etl.evaluate_data_quality(source_name, df)
        
        # ÉTAPE 3: Décision de stratégie
        print("\n🤔 ÉTAPE 3: STRATÉGIE DE TRAITEMENT")
        print("-" * 40)
        
        strategy = etl.decide_cleaning_strategy(quality_reports)
        
        # ÉTAPE 4: Amélioration (conditionnelle)
        if strategy == 'improve':
            print("\n🧹 ÉTAPE 4: AMÉLIORATION DE LA QUALITÉ")
            print("-" * 40)
            datasets = etl.improve_data_quality(datasets)
            
            # Réévaluation après amélioration
            print("\n🔍 RÉÉVALUATION POST-AMÉLIORATION")
            print("-" * 35)
            for source_name, df in datasets.items():
                post_report = etl.evaluate_data_quality(f"{source_name}_improved", df)
                improvement = (post_report['quality_score']['overall_quality_score'] - 
                             quality_reports[source_name]['quality_score']['overall_quality_score'])
                print(f"   {source_name}: +{improvement:.1f} points de qualité")
        
        # ÉTAPE 5: Intégration
        print("\n🔗 ÉTAPE 5: INTÉGRATION DES DONNÉES")
        print("-" * 40)
        
        target_tables = etl.integrate_data(datasets)
        
        # ÉTAPE 6: Rapport final
        print("\n📊 ÉTAPE 6: RAPPORT FINAL")
        print("-" * 40)
        
        final_report = etl.generate_final_report(target_tables)
        
        # Affichage du résumé
        print("\n✅ ETL TERMINÉ AVEC SUCCÈS!")
        print("=" * 50)
        print(f"⏱️  Durée totale: {final_report['etl_execution']['duration_seconds']:.1f} secondes")
        print(f"📂 Tables créées: {len(final_report['target_tables'])}")
        
        for table_name, info in final_report['target_tables'].items():
            print(f"   📊 {table_name}: {info['rows']:,} lignes")
        
        print(f"\n📁 Fichiers de sortie dans:")
        print(f"   📂 {OUTPUT_PATH}/          (données finales)")
        print(f"   📊 {QUALITY_REPORTS_PATH}/ (rapports qualité)")
        print(f"   📈 {VISUALIZATIONS_PATH}/  (graphiques)")
        
        # Ouvrir les fichiers de sortie
        print(f"\n🔍 APERÇU DES RÉSULTATS:")
        for table_name in target_tables.keys():
            file_path = f"{OUTPUT_PATH}/{table_name}.csv"
            if os.path.exists(file_path):
                df_preview = pd.read_csv(file_path)
                print(f"\n📊 {table_name.upper()}:")
                print(df_preview.head(3).to_string(index=False))
        
    except Exception as e:
        print(f"\n❌ ERREUR DURANT L'ETL: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_local_etl()