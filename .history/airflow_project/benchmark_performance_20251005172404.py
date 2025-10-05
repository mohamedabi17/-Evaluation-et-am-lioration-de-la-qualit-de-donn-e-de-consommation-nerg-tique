#!/usr/bin/env python3
"""
Moniteur de performance pour les opérations ETL sur gros volumes
Ce script mesure les performances des opérations de qualité sur différentes tailles de données
"""

import time
import psutil
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json
from datetime import datetime
import sys
import os

# Ajouter le répertoire utils au path
sys.path.append('utils')

from quality_checks import generate_quality_report, create_quality_visualizations
from data_quality_improver import DataQualityImprover, create_improvement_config_population, create_improvement_config_consommation

class ETLPerformanceMonitor:
    """Moniteur de performance pour les opérations ETL."""
    
    def __init__(self):
        self.metrics = []
        self.start_time = None
        self.start_memory = None
        self.start_cpu = None
    
    def start_monitoring(self, operation_name):
        """Démarrer le monitoring d'une opération."""
        self.start_time = time.time()
        self.start_memory = psutil.virtual_memory().used / (1024**2)  # MB
        self.start_cpu = psutil.cpu_percent()
        
        return {
            'operation': operation_name,
            'start_time': self.start_time,
            'start_memory_mb': self.start_memory,
            'start_cpu_percent': self.start_cpu
        }
    
    def stop_monitoring(self, operation_data, rows_processed=0, additional_metrics=None):
        """Arrêter le monitoring et enregistrer les métriques."""
        end_time = time.time()
        end_memory = psutil.virtual_memory().used / (1024**2)
        end_cpu = psutil.cpu_percent()
        
        duration = end_time - operation_data['start_time']
        memory_usage = end_memory - operation_data['start_memory_mb']
        
        metrics = {
            'operation': operation_data['operation'],
            'duration_seconds': round(duration, 2),
            'memory_usage_mb': round(memory_usage, 2),
            'cpu_usage_percent': round((operation_data['start_cpu_percent'] + end_cpu) / 2, 2),
            'rows_processed': rows_processed,
            'rows_per_second': round(rows_processed / duration, 2) if duration > 0 else 0,
            'timestamp': datetime.now().isoformat()
        }
        
        if additional_metrics:
            metrics.update(additional_metrics)
        
        self.metrics.append(metrics)
        return metrics

def benchmark_data_loading():
    """Benchmark du chargement de données."""
    
    print("📊 BENCHMARK - Chargement de Données")
    print("-" * 40)
    
    monitor = ETLPerformanceMonitor()
    sources_dir = Path("sources")
    
    if not sources_dir.exists():
        print("❌ Dossier 'sources' non trouvé")
        return {}
    
    results = {}
    
    for csv_file in sources_dir.glob("*.csv"):
        print(f"📂 Chargement de {csv_file.name}...")
        
        # Mesurer la taille du fichier
        file_size_mb = csv_file.stat().st_size / (1024**2)
        
        # Démarrer le monitoring
        op_data = monitor.start_monitoring(f"load_{csv_file.stem}")
        
        try:
            # Charger les données
            df = pd.read_csv(csv_file)
            
            # Arrêter le monitoring
            metrics = monitor.stop_monitoring(
                op_data, 
                rows_processed=len(df),
                additional_metrics={
                    'file_size_mb': round(file_size_mb, 2),
                    'columns': len(df.columns),
                    'mb_per_second': round(file_size_mb / (time.time() - op_data['start_time']), 2)
                }
            )
            
            results[csv_file.stem] = metrics
            
            print(f"  ✅ {len(df):,} lignes en {metrics['duration_seconds']}s "
                  f"({metrics['rows_per_second']:,.0f} lignes/s)")
            
        except Exception as e:
            print(f"  ❌ Erreur: {e}")
    
    return results

def benchmark_quality_evaluation():
    """Benchmark de l'évaluation de qualité."""
    
    print("\n🔍 BENCHMARK - Évaluation de Qualité")
    print("-" * 40)
    
    monitor = ETLPerformanceMonitor()
    sources_dir = Path("sources")
    results = {}
    
    for csv_file in sources_dir.glob("*.csv"):
        if csv_file.name.startswith(('population', 'consommation')):
            print(f"🔍 Évaluation qualité de {csv_file.name}...")
            
            # Charger les données
            df = pd.read_csv(csv_file)
            
            # Démarrer le monitoring
            op_data = monitor.start_monitoring(f"quality_{csv_file.stem}")
            
            try:
                # Générer le rapport de qualité
                report = generate_quality_report(df, csv_file.stem)
                
                # Arrêter le monitoring
                metrics = monitor.stop_monitoring(
                    op_data,
                    rows_processed=len(df),
                    additional_metrics={
                        'quality_score': report['quality_score']['overall_quality_score'],
                        'completeness_score': report['quality_score']['completeness_score'],
                        'uniqueness_score': report['quality_score']['uniqueness_score']
                    }
                )
                
                results[csv_file.stem] = metrics
                
                print(f"  ✅ Score qualité: {metrics['quality_score']:.1f}% "
                      f"en {metrics['duration_seconds']}s")
                
            except Exception as e:
                print(f"  ❌ Erreur: {e}")
    
    return results

def benchmark_data_improvement():
    """Benchmark de l'amélioration de qualité."""
    
    print("\n🧹 BENCHMARK - Amélioration de Qualité")
    print("-" * 40)
    
    monitor = ETLPerformanceMonitor()
    sources_dir = Path("sources")
    results = {}
    
    for csv_file in sources_dir.glob("population*.csv"):
        print(f"🧹 Amélioration de {csv_file.name}...")
        
        # Charger les données
        df = pd.read_csv(csv_file)
        original_quality = generate_quality_report(df, "temp")['quality_score']['overall_quality_score']
        
        # Démarrer le monitoring
        op_data = monitor.start_monitoring(f"improve_{csv_file.stem}")
        
        try:
            # Appliquer les améliorations
            improver = DataQualityImprover()
            config = create_improvement_config_population()
            
            # Série d'améliorations
            df_improved = improver.remove_duplicates(df)
            df_improved = improver.improve_completeness(df_improved, config['completeness_strategies'])
            df_improved = improver.standardize_format(df_improved, config['format_rules'])
            df_improved = improver.normalize_codification(df_improved, config['codification_rules'])
            df_improved = improver.apply_business_rules(df_improved, config['business_rules'])
            
            # Calculer la qualité finale
            final_quality = generate_quality_report(df_improved, "temp")['quality_score']['overall_quality_score']
            quality_gain = final_quality - original_quality
            
            # Arrêter le monitoring
            metrics = monitor.stop_monitoring(
                op_data,
                rows_processed=len(df),
                additional_metrics={
                    'original_quality': round(original_quality, 1),
                    'final_quality': round(final_quality, 1),
                    'quality_gain': round(quality_gain, 1),
                    'improvement_actions': len(improver.improvement_log)
                }
            )
            
            results[csv_file.stem] = metrics
            
            print(f"  ✅ Qualité: {original_quality:.1f}% → {final_quality:.1f}% "
                  f"(+{quality_gain:.1f}) en {metrics['duration_seconds']}s")
            
        except Exception as e:
            print(f"  ❌ Erreur: {e}")
    
    return results

def benchmark_aggregations():
    """Benchmark des opérations d'agrégation."""
    
    print("\n📈 BENCHMARK - Agrégations")
    print("-" * 40)
    
    monitor = ETLPerformanceMonitor()
    sources_dir = Path("sources")
    
    # Charger toutes les données nécessaires
    try:
        pop_paris = pd.read_csv(sources_dir / "population_paris.csv")
        pop_evry = pd.read_csv(sources_dir / "population_evry.csv")
        conso_paris = pd.read_csv(sources_dir / "consommation_paris.csv")
        conso_evry = pd.read_csv(sources_dir / "consommation_evry.csv")
        
        results = {}
        
        # 1. Agrégation par CSP
        print("📊 Agrégation par CSP...")
        op_data = monitor.start_monitoring("aggregation_csp")
        
        pop_all = pd.concat([pop_paris, pop_evry])
        conso_all = pd.concat([conso_paris, conso_evry])
        
        # Jointure simplifiée pour le benchmark
        merged = pd.merge(
            pop_all[['ID_Personne', 'CSP']], 
            conso_all[['ID_Adr', 'NB_KW_Jour']], 
            left_on='ID_Personne', 
            right_on='ID_Adr', 
            how='inner'
        )
        
        # Agrégation
        agg_csp = merged.groupby('CSP')['NB_KW_Jour'].agg(['mean', 'count', 'std']).reset_index()
        
        metrics = monitor.stop_monitoring(
            op_data,
            rows_processed=len(merged),
            additional_metrics={
                'input_rows': len(pop_all) + len(conso_all),
                'joined_rows': len(merged),
                'output_groups': len(agg_csp)
            }
        )
        
        results['aggregation_csp'] = metrics
        print(f"  ✅ {len(merged):,} lignes → {len(agg_csp)} groupes CSP "
              f"en {metrics['duration_seconds']}s")
        
        # 2. Agrégation géographique (simulation)
        print("🗺️  Agrégation géographique...")
        op_data = monitor.start_monitoring("aggregation_geo")
        
        # Simulation d'agrégation géographique
        geo_agg = conso_all.groupby('Code_Postal')['NB_KW_Jour'].agg(['mean', 'count', 'min', 'max']).reset_index()
        
        metrics = monitor.stop_monitoring(
            op_data,
            rows_processed=len(conso_all),
            additional_metrics={
                'input_rows': len(conso_all),
                'output_groups': len(geo_agg)
            }
        )
        
        results['aggregation_geo'] = metrics
        print(f"  ✅ {len(conso_all):,} lignes → {len(geo_agg)} groupes géographiques "
              f"en {metrics['duration_seconds']}s")
        
        return results
        
    except Exception as e:
        print(f"❌ Erreur lors des agrégations: {e}")
        return {}

def create_performance_dashboard(all_results):
    """Créer un dashboard de performance."""
    
    print("\n📊 Création du Dashboard de Performance...")
    
    # Préparer les données pour visualisation
    operations = []
    durations = []
    rows_processed = []
    memory_usage = []
    
    for category, results in all_results.items():
        for operation, metrics in results.items():
            operations.append(f"{category}_{operation}")
            durations.append(metrics['duration_seconds'])
            rows_processed.append(metrics['rows_processed'])
            memory_usage.append(metrics['memory_usage_mb'])
    
    # Créer les graphiques
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Durées par opération
    ax1.bar(operations, durations, color='skyblue', alpha=0.7)
    ax1.set_title('Durée par Opération (secondes)')
    ax1.set_ylabel('Durée (s)')
    ax1.tick_params(axis='x', rotation=45)
    
    # 2. Débit (lignes/seconde)
    throughput = [r/d if d > 0 else 0 for r, d in zip(rows_processed, durations)]
    ax2.bar(operations, throughput, color='lightgreen', alpha=0.7)
    ax2.set_title('Débit (lignes/seconde)')
    ax2.set_ylabel('Lignes/s')
    ax2.tick_params(axis='x', rotation=45)
    
    # 3. Utilisation mémoire
    ax3.bar(operations, memory_usage, color='coral', alpha=0.7)
    ax3.set_title('Utilisation Mémoire (MB)')
    ax3.set_ylabel('Mémoire (MB)')
    ax3.tick_params(axis='x', rotation=45)
    
    # 4. Performance vs Taille
    ax4.scatter(rows_processed, durations, c=memory_usage, cmap='viridis', alpha=0.7, s=100)
    ax4.set_xlabel('Lignes traitées')
    ax4.set_ylabel('Durée (s)')
    ax4.set_title('Performance vs Taille des Données')
    ax4.set_xscale('log')
    
    plt.tight_layout()
    
    # Sauvegarder
    viz_dir = Path("visualizations")
    viz_dir.mkdir(exist_ok=True)
    plt.savefig(viz_dir / "performance_dashboard.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✅ Dashboard sauvegardé: {viz_dir}/performance_dashboard.png")

def save_performance_report(all_results):
    """Sauvegarder un rapport de performance détaillé."""
    
    reports_dir = Path("quality_reports")
    reports_dir.mkdir(exist_ok=True)
    
    # Calculer des statistiques globales
    all_metrics = []
    for category_results in all_results.values():
        all_metrics.extend(category_results.values())
    
    if not all_metrics:
        return
    
    total_duration = sum(m['duration_seconds'] for m in all_metrics)
    total_rows = sum(m['rows_processed'] for m in all_metrics)
    avg_throughput = total_rows / total_duration if total_duration > 0 else 0
    max_memory = max(m['memory_usage_mb'] for m in all_metrics)
    
    performance_report = {
        'benchmark_timestamp': datetime.now().isoformat(),
        'system_info': {
            'cpu_count': psutil.cpu_count(),
            'memory_total_gb': round(psutil.virtual_memory().total / (1024**3), 2),
            'python_version': sys.version
        },
        'global_metrics': {
            'total_duration_seconds': round(total_duration, 2),
            'total_rows_processed': total_rows,
            'average_throughput_rows_per_second': round(avg_throughput, 2),
            'peak_memory_usage_mb': round(max_memory, 2)
        },
        'detailed_results': all_results
    }
    
    # Sauvegarder le rapport
    report_file = reports_dir / f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(performance_report, f, indent=2, ensure_ascii=False)
    
    print(f"\n📋 Rapport de performance sauvegardé: {report_file}")
    
    # Afficher le résumé
    print("\n📊 RÉSUMÉ DE PERFORMANCE")
    print("-" * 30)
    print(f"Durée totale: {total_duration:.1f}s")
    print(f"Lignes traitées: {total_rows:,}")
    print(f"Débit moyen: {avg_throughput:,.0f} lignes/s")
    print(f"Pic mémoire: {max_memory:.1f} MB")

def main():
    """Fonction principale de benchmark."""
    
    print("🚀 BENCHMARK DE PERFORMANCE ETL")
    print("=" * 50)
    print("Ce benchmark va mesurer les performances des opérations ETL")
    print("sur les données présentes dans le dossier 'sources'")
    print()
    
    # Vérifier la présence de données
    sources_dir = Path("sources")
    if not sources_dir.exists() or not any(sources_dir.glob("*.csv")):
        print("❌ Aucune donnée trouvée dans 'sources'!")
        print("Générez d'abord des données avec generate_large_datasets.py")
        return
    
    # Afficher les données disponibles
    print("📂 Données disponibles:")
    total_size = 0
    for csv_file in sources_dir.glob("*.csv"):
        size_mb = csv_file.stat().st_size / (1024**2)
        total_size += size_mb
        print(f"  {csv_file.name:25} : {size_mb:6.1f} MB")
    print(f"  {'TOTAL':25} : {total_size:6.1f} MB")
    print()
    
    # Demander confirmation
    proceed = input("Lancer le benchmark? (o/N): ").lower().strip()
    if proceed not in ['o', 'oui', 'y', 'yes']:
        print("❌ Benchmark annulé")
        return
    
    start_time = time.time()
    
    # Lancer les différents benchmarks
    all_results = {}
    
    try:
        # 1. Benchmark chargement
        all_results['loading'] = benchmark_data_loading()
        
        # 2. Benchmark évaluation qualité
        all_results['quality_evaluation'] = benchmark_quality_evaluation()
        
        # 3. Benchmark amélioration
        all_results['quality_improvement'] = benchmark_data_improvement()
        
        # 4. Benchmark agrégations
        all_results['aggregations'] = benchmark_aggregations()
        
        # 5. Créer les visualisations
        create_performance_dashboard(all_results)
        
        # 6. Sauvegarder le rapport
        save_performance_report(all_results)
        
        total_time = time.time() - start_time
        print(f"\n✅ Benchmark terminé en {total_time:.1f}s")
        
    except Exception as e:
        print(f"\n❌ Erreur durant le benchmark: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()