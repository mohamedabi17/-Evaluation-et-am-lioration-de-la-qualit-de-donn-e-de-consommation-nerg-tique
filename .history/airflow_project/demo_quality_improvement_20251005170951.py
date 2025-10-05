#!/usr/bin/env python3
"""
Script de démonstration des améliorations de qualité des données
Ce script peut être exécuté indépendamment pour tester les fonctionnalités
"""

import sys
import os
import pandas as pd
import json

# Ajouter le répertoire utils au path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from quality_checks import generate_quality_report, create_quality_visualizations
from data_quality_improver import DataQualityImprover, create_improvement_config_population

def demonstrate_quality_improvement():
    """Démonstration complète du processus d'amélioration qualité."""
    
    print("🚀 Démonstration des Améliorations de Qualité des Données")
    print("=" * 60)
    
    # 1. Charger les données sources avec défauts
    sources_path = "sources"
    pop_paris_path = os.path.join(sources_path, "population_paris.csv")
    
    if not os.path.exists(pop_paris_path):
        print(f"❌ Fichier {pop_paris_path} non trouvé")
        return
    
    df_original = pd.read_csv(pop_paris_path)
    print(f"📊 Données originales chargées: {len(df_original)} lignes")
    
    # 2. Évaluation qualité initiale
    print("\n🔍 ÉVALUATION QUALITÉ INITIALE")
    print("-" * 40)
    
    initial_report = generate_quality_report(df_original, "population_paris_original")
    initial_score = initial_report['quality_score']['overall_quality_score']
    
    print(f"Score global initial: {initial_score}% ({initial_report['quality_score']['quality_level']})")
    print(f"Complétude: {initial_report['quality_score']['completeness_score']}%")
    print(f"Unicité: {initial_report['quality_score']['uniqueness_score']}%")
    print(f"Codification: {initial_report['quality_score']['codification_score']}%")
    
    # Afficher les problèmes détectés
    print("\n📋 Problèmes détectés:")
    for col, completeness in initial_report['completeness'].items():
        if completeness['missing_count'] > 0:
            print(f"  - {col}: {completeness['missing_count']} valeurs manquantes")
    
    duplicates = initial_report['duplicates']['duplicate_count']
    if duplicates > 0:
        print(f"  - {duplicates} doublons détectés")
    
    # 3. Application des améliorations
    print("\n🧹 APPLICATION DES AMÉLIORATIONS")
    print("-" * 40)
    
    improver = DataQualityImprover()
    config = create_improvement_config_population()
    
    # Appliquer les améliorations étape par étape
    df_improved = df_original.copy()
    
    # Élimination des doublons
    df_improved = improver.remove_duplicates(df_improved)
    
    # Amélioration de la complétude
    df_improved = improver.improve_completeness(df_improved, config['completeness_strategies'])
    
    # Standardisation des formats
    df_improved = improver.standardize_format(df_improved, config['format_rules'])
    
    # Normalisation de la codification
    df_improved = improver.normalize_codification(df_improved, config['codification_rules'])
    
    # Application des règles métier
    df_improved = improver.apply_business_rules(df_improved, config['business_rules'])
    
    # 4. Évaluation qualité finale
    print("\n📈 ÉVALUATION QUALITÉ FINALE")
    print("-" * 40)
    
    final_report = generate_quality_report(df_improved, "population_paris_improved")
    final_score = final_report['quality_score']['overall_quality_score']
    
    print(f"Score global final: {final_score}% ({final_report['quality_score']['quality_level']})")
    print(f"Complétude: {final_report['quality_score']['completeness_score']}%")
    print(f"Unicité: {final_report['quality_score']['uniqueness_score']}%")
    print(f"Codification: {final_report['quality_score']['codification_score']}%")
    
    # 5. Résumé des améliorations
    improvement_gain = final_score - initial_score
    print(f"\n🎯 GAIN QUALITÉ: +{improvement_gain:.1f} points")
    
    print("\n📝 Résumé des améliorations appliquées:")
    summary = improver.get_improvement_summary()
    for category, count in summary['by_category'].items():
        print(f"  - {category}: {count} action(s)")
    
    # 6. Sauvegarder les résultats
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Sauvegarder les données améliorées
    improved_file = os.path.join(output_dir, "population_paris_improved_demo.csv")
    df_improved.to_csv(improved_file, index=False)
    print(f"\n💾 Données améliorées sauvegardées: {improved_file}")
    
    # Sauvegarder les rapports
    reports_dir = "quality_reports"
    os.makedirs(reports_dir, exist_ok=True)
    
    with open(os.path.join(reports_dir, "demo_initial_report.json"), 'w', encoding='utf-8') as f:
        json.dump(initial_report, f, indent=2, ensure_ascii=False)
    
    with open(os.path.join(reports_dir, "demo_final_report.json"), 'w', encoding='utf-8') as f:
        json.dump(final_report, f, indent=2, ensure_ascii=False)
    
    # Créer les visualisations
    viz_dir = "visualizations"
    os.makedirs(viz_dir, exist_ok=True)
    
    try:
        create_quality_visualizations(initial_report, viz_dir)
        create_quality_visualizations(final_report, viz_dir)
        print(f"📊 Visualisations créées dans {viz_dir}")
    except Exception as e:
        print(f"⚠️  Erreur lors de la création des visualisations: {e}")
    
    print("\n✅ Démonstration terminée avec succès!")
    print(f"📈 Amélioration globale: {initial_score:.1f}% → {final_score:.1f}% (+{improvement_gain:.1f})")

def analyze_all_sources():
    """Analyser toutes les sources de données."""
    
    print("\n🔍 ANALYSE COMPLÈTE DE TOUTES LES SOURCES")
    print("=" * 50)
    
    sources = [
        "population_paris.csv",
        "population_evry.csv", 
        "consommation_paris.csv",
        "consommation_evry.csv",
        "csp.csv",
        "iris.csv"
    ]
    
    results = {}
    
    for source in sources:
        source_path = os.path.join("sources", source)
        if os.path.exists(source_path):
            df = pd.read_csv(source_path)
            source_name = source.replace('.csv', '')
            report = generate_quality_report(df, source_name)
            results[source_name] = report['quality_score']['overall_quality_score']
            
            print(f"{source_name:25} : {report['quality_score']['overall_quality_score']:5.1f}% ({report['quality_score']['quality_level']})")
    
    # Score moyen
    avg_score = sum(results.values()) / len(results)
    print(f"\n{'SCORE MOYEN':25} : {avg_score:5.1f}%")
    
    # Identifier les sources problématiques
    problematic = [name for name, score in results.items() if score < 80]
    if problematic:
        print(f"\n⚠️  Sources nécessitant une amélioration: {', '.join(problematic)}")
    else:
        print(f"\n✅ Toutes les sources ont une qualité acceptable (>80%)")

if __name__ == "__main__":
    print("🎯 Script de Démonstration - Qualité des Données Énergétiques")
    print("Ce script démontre les capacités d'évaluation et d'amélioration de la qualité")
    print()
    
    try:
        # Démonstration principale
        demonstrate_quality_improvement()
        
        # Analyse de toutes les sources
        analyze_all_sources()
        
    except Exception as e:
        print(f"❌ Erreur durante la démonstration: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n🏁 Fin de la démonstration")