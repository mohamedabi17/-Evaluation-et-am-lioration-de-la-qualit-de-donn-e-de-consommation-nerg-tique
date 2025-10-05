#!/usr/bin/env python3
"""
📊 ANALYSEUR D'AMÉLIORATION DE QUALITÉ
Affiche les détails des améliorations avec seuil d'acceptabilité à 99%
"""

import os
import sys
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# Ajouter le répertoire utils au path
sys.path.append('utils')

from quality_checks import generate_quality_report
from data_quality_improver import DataQualityImprover, create_improvement_config_population, create_improvement_config_consommation

def analyze_quality_improvement(source_file, source_name):
    """Analyser et afficher l'amélioration de qualité pour une source."""
    
    print(f"\n📊 ANALYSE DÉTAILLÉE: {source_name.upper()}")
    print("=" * 60)
    
    # Charger les données originales
    if not os.path.exists(source_file):
        print(f"❌ Fichier source non trouvé: {source_file}")
        return
    
    df_original = pd.read_csv(source_file)
    print(f"📁 Données originales: {len(df_original):,} lignes × {len(df_original.columns)} colonnes")
    
    # Évaluation qualité AVANT amélioration
    print("\n🔍 ÉVALUATION AVANT AMÉLIORATION")
    print("-" * 40)
    
    report_before = generate_quality_report(df_original, f"{source_name}_before", None)
    score_before = report_before['quality_score']['overall_quality_score']
    
    print(f"   🎯 Score global: {score_before:.2f}%")
    print(f"   ✅ Complétude: {report_before['quality_score']['completeness_score']:.2f}%")
    print(f"   🔄 Unicité: {report_before['quality_score']['uniqueness_score']:.2f}%")
    
    # Détails des problèmes détectés
    print("\n   📋 PROBLÈMES DÉTECTÉS:")
    completeness = report_before['completeness']
    for col, info in completeness.items():
        if info['missing_count'] > 0:
            print(f"      • {col}: {info['missing_count']} valeurs manquantes ({info['missing_percentage']:.1f}%)")
    
    duplicates = report_before['duplicates']
    if duplicates['duplicate_count'] > 0:
        print(f"      • Doublons: {duplicates['duplicate_count']} lignes ({duplicates['duplicate_percentage']:.1f}%)")
    
    # Application des améliorations
    print(f"\n🧹 APPLICATION DES AMÉLIORATIONS")
    print("-" * 40)
    
    improver = DataQualityImprover()
    
    # Configuration selon le type de données
    if 'population' in source_name:
        config = create_improvement_config_population()
    else:
        config = create_improvement_config_consommation()
    
    # Étapes d'amélioration détaillées
    df_improved = df_original.copy()
    
    # 1. Suppression des doublons
    print("   1️⃣ Suppression des doublons...")
    duplicates_before = len(df_improved)
    df_improved = improver.remove_duplicates(df_improved)
    duplicates_removed = duplicates_before - len(df_improved)
    print(f"      ✅ {duplicates_removed} doublons supprimés")
    
    # 2. Amélioration de la complétude
    print("   2️⃣ Amélioration de la complétude...")
    missing_before = df_improved.isnull().sum().sum()
    df_improved = improver.improve_completeness(df_improved, config['completeness_strategies'])
    missing_after = df_improved.isnull().sum().sum()
    print(f"      ✅ {missing_before - missing_after} valeurs manquantes comblées")
    
    # 3. Standardisation des formats
    print("   3️⃣ Standardisation des formats...")
    df_improved = improver.standardize_format(df_improved, config['format_rules'])
    print(f"      ✅ {len(config['format_rules'])} colonnes formatées")
    
    # 4. Normalisation de la codification (si applicable)
    if 'codification_rules' in config and config['codification_rules']:
        print("   4️⃣ Normalisation de la codification...")
        df_improved = improver.normalize_codification(df_improved, config['codification_rules'])
        print(f"      ✅ {len(config['codification_rules'])} colonnes codifiées")
    
    # 5. Application des règles métier
    print("   5️⃣ Application des règles métier...")
    df_improved = improver.apply_business_rules(df_improved, config['business_rules'])
    print(f"      ✅ {len(config['business_rules'])} règles appliquées")
    
    # Résumé des améliorations
    improvement_summary = improver.get_improvement_summary()
    total_actions = sum(improvement_summary['by_category'].values())
    
    print(f"\n   📊 RÉSUMÉ DES ACTIONS ({total_actions} total):")
    for category, count in improvement_summary['by_category'].items():
        print(f"      • {category}: {count} action(s)")
    
    # Évaluation qualité APRÈS amélioration
    print(f"\n🔍 ÉVALUATION APRÈS AMÉLIORATION")
    print("-" * 40)
    
    report_after = generate_quality_report(df_improved, f"{source_name}_after", None)
    score_after = report_after['quality_score']['overall_quality_score']
    
    print(f"   🎯 Score global: {score_after:.2f}%")
    print(f"   ✅ Complétude: {report_after['quality_score']['completeness_score']:.2f}%")
    print(f"   🔄 Unicité: {report_after['quality_score']['uniqueness_score']:.2f}%")
    
    # Calcul de l'amélioration
    improvement = score_after - score_before
    improvement_pct = (improvement / score_before) * 100 if score_before > 0 else 0
    
    print(f"\n📈 AMÉLIORATION RÉALISÉE")
    print("-" * 30)
    print(f"   📊 Gain de qualité: +{improvement:.2f} points ({improvement_pct:+.1f}%)")
    print(f"   📏 Données finales: {len(df_improved):,} lignes")
    
    # Évaluation par rapport au seuil de 99%
    quality_threshold = 99.0
    print(f"\n🎯 ÉVALUATION SEUIL D'ACCEPTABILITÉ ({quality_threshold}%)")
    print("-" * 50)
    
    if score_after >= quality_threshold:
        print(f"   ✅ EXCELLENT: {score_after:.2f}% ≥ {quality_threshold}%")
        print("   🎉 Qualité conforme aux standards!")
    else:
        gap = quality_threshold - score_after
        print(f"   ⚠️  INSUFFISANT: {score_after:.2f}% < {quality_threshold}%")
        print(f"   📉 Écart: -{gap:.2f} points")
        print("   💡 Améliorations supplémentaires recommandées")
    
    # Sauvegarder les données améliorées
    output_file = f"output/{source_name}_improved_detailed.csv"
    Path("output").mkdir(exist_ok=True)
    df_improved.to_csv(output_file, index=False)
    print(f"\n💾 Données améliorées sauvegardées: {output_file}")
    
    return {
        'source_name': source_name,
        'score_before': score_before,
        'score_after': score_after,
        'improvement': improvement,
        'improvement_percentage': improvement_pct,
        'meets_threshold': score_after >= quality_threshold,
        'total_actions': total_actions,
        'final_rows': len(df_improved)
    }

def main():
    """Analyser l'amélioration de qualité pour toutes les sources."""
    
    print("🎯 ANALYSE DÉTAILLÉE D'AMÉLIORATION DE QUALITÉ")
    print("=" * 70)
    print("Seuil d'acceptabilité: 99%")
    print()
    
    # Vérifier le répertoire de travail
    sources_dir = Path("sources")
    if not sources_dir.exists():
        print("❌ Dossier 'sources' non trouvé")
        print("💡 Assurez-vous d'être dans le répertoire du projet")
        return
    
    # Sources à analyser
    sources = [
        ('sources/population_paris.csv', 'population_paris'),
        ('sources/population_evry.csv', 'population_evry'),
        ('sources/consommation_paris.csv', 'consommation_paris'),
        ('sources/consommation_evry.csv', 'consommation_evry')
    ]
    
    results = []
    
    # Analyser chaque source
    for source_file, source_name in sources:
        if os.path.exists(source_file):
            result = analyze_quality_improvement(source_file, source_name)
            if result:
                results.append(result)
        else:
            print(f"❌ Fichier non trouvé: {source_file}")
    
    # Résumé global
    if results:
        print(f"\n📊 RÉSUMÉ GLOBAL DE L'AMÉLIORATION")
        print("=" * 60)
        
        total_sources = len(results)
        sources_meeting_threshold = sum(1 for r in results if r['meets_threshold'])
        avg_improvement = sum(r['improvement'] for r in results) / total_sources
        total_actions = sum(r['total_actions'] for r in results)
        
        print(f"   📋 Sources analysées: {total_sources}")
        print(f"   ✅ Sources conformes (≥99%): {sources_meeting_threshold}/{total_sources}")
        print(f"   📈 Amélioration moyenne: +{avg_improvement:.2f} points")
        print(f"   🔧 Actions totales appliquées: {total_actions}")
        
        print(f"\n   📊 DÉTAIL PAR SOURCE:")
        for result in results:
            status = "✅" if result['meets_threshold'] else "⚠️"
            print(f"      {status} {result['source_name']}: {result['score_before']:.1f}% → {result['score_after']:.1f}% (+{result['improvement']:.1f})")
        
        # Recommandations
        print(f"\n💡 RECOMMANDATIONS")
        print("-" * 25)
        
        if sources_meeting_threshold == total_sources:
            print("   🎉 Excellent! Toutes les sources respectent le seuil de 99%")
            print("   ✅ Qualité optimale atteinte")
        else:
            non_compliant = total_sources - sources_meeting_threshold
            print(f"   ⚠️  {non_compliant} source(s) ne respectent pas le seuil de 99%")
            print("   🔧 Améliorations supplémentaires recommandées:")
            
            for result in results:
                if not result['meets_threshold']:
                    gap = 99.0 - result['score_after']
                    print(f"      • {result['source_name']}: -{gap:.1f} points à gagner")
    
    print(f"\n🎯 Analyse terminée!")

if __name__ == "__main__":
    main()