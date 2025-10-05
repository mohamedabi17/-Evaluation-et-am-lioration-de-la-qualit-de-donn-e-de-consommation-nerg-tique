#!/usr/bin/env python3
"""
🎯 ETL SIMPLIFIÉ - Focus sur l'amélioration de qualité avec seuil 99%
Évite les problèmes de jointure et se concentre sur la qualité
"""

import os
import sys
import pandas as pd
import json
from datetime import datetime
from pathlib import Path

# Ajouter le répertoire utils au path
sys.path.append('utils')

from quality_checks import generate_quality_report, create_quality_visualizations
from data_quality_improver import (
    DataQualityImprover,
    create_improvement_config_population,
    create_improvement_config_consommation
)

# Configuration
SOURCES_PATH = "sources"
OUTPUT_PATH = "output"
QUALITY_REPORTS_PATH = "quality_reports" 
VISUALIZATIONS_PATH = "visualizations"
QUALITY_THRESHOLD = 99.0  # Seuil d'acceptabilité à 99%

class SimplifiedETL:
    """ETL simplifié focus qualité."""
    
    def __init__(self):
        self.create_directories()
        self.execution_log = []
        self.start_time = datetime.now()
    
    def create_directories(self):
        """Créer les dossiers de sortie."""
        for path in [OUTPUT_PATH, QUALITY_REPORTS_PATH, VISUALIZATIONS_PATH]:
            Path(path).mkdir(exist_ok=True)
        print("📁 Dossiers de sortie créés")
    
    def log_step(self, message):
        """Logger une étape."""
        timestamp = datetime.now()
        print(f"✅ {message} - {timestamp.strftime('%H:%M:%S')}")
        self.execution_log.append({
            'message': message,
            'timestamp': timestamp.isoformat()
        })
    
    def process_source(self, source_name):
        """Traiter une source de données complètement."""
        
        print(f"\n🔄 TRAITEMENT: {source_name.upper()}")
        print("=" * 50)
        
        # 1. Extraction
        file_path = f"{SOURCES_PATH}/{source_name}.csv"
        if not os.path.exists(file_path):
            print(f"❌ Fichier non trouvé: {file_path}")
            return None
        
        df_original = pd.read_csv(file_path)
        print(f"📊 Données extraites: {len(df_original):,} lignes × {len(df_original.columns)} colonnes")
        
        # 2. Évaluation qualité AVANT
        print("\n🔍 ÉVALUATION QUALITÉ INITIALE")
        print("-" * 35)
        
        report_before_path = f"{QUALITY_REPORTS_PATH}/{source_name}_before_quality.json"
        report_before = generate_quality_report(df_original, f"{source_name}_before", report_before_path)
        
        score_before = report_before['quality_score']['overall_quality_score']
        completeness_before = report_before['quality_score']['completeness_score']
        uniqueness_before = report_before['quality_score']['uniqueness_score']
        
        print(f"   🎯 Score global: {score_before:.2f}%")
        print(f"   ✅ Complétude: {completeness_before:.2f}%")
        print(f"   🔄 Unicité: {uniqueness_before:.2f}%")
        
        # Analyse des problèmes
        problems = []
        for col, info in report_before['completeness'].items():
            if info['missing_count'] > 0:
                problems.append(f"{col}: {info['missing_count']} manquantes")
        
        if report_before['duplicates']['duplicate_count'] > 0:
            problems.append(f"Doublons: {report_before['duplicates']['duplicate_count']}")
        
        if problems:
            print(f"   ⚠️  Problèmes détectés: {', '.join(problems)}")
        
        # 3. Décision d'amélioration
        needs_improvement = score_before < QUALITY_THRESHOLD
        gap = QUALITY_THRESHOLD - score_before if needs_improvement else 0
        
        print(f"\n🎯 ÉVALUATION SEUIL ({QUALITY_THRESHOLD}%)")
        print("-" * 30)
        
        if needs_improvement:
            print(f"   ❌ INSUFFISANT: {score_before:.2f}% < {QUALITY_THRESHOLD}%")
            print(f"   📉 Écart: -{gap:.2f} points")
            print("   🧹 Amélioration requise")
        else:
            print(f"   ✅ CONFORME: {score_before:.2f}% ≥ {QUALITY_THRESHOLD}%")
            print("   🎉 Qualité excellente")
        
        # 4. Amélioration (si nécessaire)
        if needs_improvement:
            print(f"\n🧹 AMÉLIORATION DE LA QUALITÉ")
            print("-" * 35)
            
            df_improved = self.improve_data_quality(df_original, source_name)
            
            # Évaluation qualité APRÈS
            print(f"\n🔍 ÉVALUATION QUALITÉ FINALE")
            print("-" * 30)
            
            report_after_path = f"{QUALITY_REPORTS_PATH}/{source_name}_after_quality.json"
            report_after = generate_quality_report(df_improved, f"{source_name}_after", report_after_path)
            
            score_after = report_after['quality_score']['overall_quality_score']
            completeness_after = report_after['quality_score']['completeness_score']
            uniqueness_after = report_after['quality_score']['uniqueness_score']
            
            print(f"   🎯 Score global: {score_after:.2f}%")
            print(f"   ✅ Complétude: {completeness_after:.2f}%")
            print(f"   🔄 Unicité: {uniqueness_after:.2f}%")
            
            # Calcul de l'amélioration
            improvement = score_after - score_before
            improvement_pct = (improvement / score_before) * 100 if score_before > 0 else 0
            
            print(f"\n📈 AMÉLIORATION RÉALISÉE")
            print("-" * 28)
            print(f"   📊 Gain: +{improvement:.2f} points ({improvement_pct:+.1f}%)")
            
            # Validation finale
            meets_threshold = score_after >= QUALITY_THRESHOLD
            final_gap = QUALITY_THRESHOLD - score_after if not meets_threshold else 0
            
            if meets_threshold:
                print(f"   ✅ SUCCÈS: {score_after:.2f}% ≥ {QUALITY_THRESHOLD}%")
                print("   🎉 Objectif qualité atteint!")
            else:
                print(f"   ⚠️  PARTIEL: {score_after:.2f}% < {QUALITY_THRESHOLD}%")
                print(f"   📉 Écart restant: -{final_gap:.2f} points")
            
            # Sauvegarder les données améliorées
            output_file = f"{OUTPUT_PATH}/{source_name}_improved.csv"
            df_improved.to_csv(output_file, index=False)
            print(f"   💾 Sauvegardé: {output_file}")
            
            return {
                'source_name': source_name,
                'original_rows': len(df_original),
                'final_rows': len(df_improved),
                'score_before': score_before,
                'score_after': score_after,
                'improvement': improvement,
                'meets_threshold': meets_threshold,
                'file_saved': output_file
            }
        
        else:
            # Pas d'amélioration nécessaire
            output_file = f"{OUTPUT_PATH}/{source_name}_validated.csv"
            df_original.to_csv(output_file, index=False)
            print(f"   💾 Sauvegardé (sans modification): {output_file}")
            
            return {
                'source_name': source_name,
                'original_rows': len(df_original),
                'final_rows': len(df_original),
                'score_before': score_before,
                'score_after': score_before,
                'improvement': 0,
                'meets_threshold': True,
                'file_saved': output_file
            }
    
    def improve_data_quality(self, df, source_name):
        """Améliorer la qualité des données."""
        
        improver = DataQualityImprover()
        
        # Configuration selon le type
        if 'population' in source_name:
            config = create_improvement_config_population()
        else:
            config = create_improvement_config_consommation()
        
        df_improved = df.copy()
        
        # Étape 1: Suppression des doublons
        print("   1️⃣ Suppression des doublons...")
        duplicates_before = len(df_improved)
        df_improved = improver.remove_duplicates(df_improved)
        duplicates_removed = duplicates_before - len(df_improved)
        print(f"      ✅ {duplicates_removed} doublons supprimés")
        
        # Étape 2: Amélioration de la complétude
        print("   2️⃣ Comblement des valeurs manquantes...")
        missing_before = df_improved.isnull().sum().sum()
        df_improved = improver.improve_completeness(df_improved, config['completeness_strategies'])
        missing_after = df_improved.isnull().sum().sum()
        print(f"      ✅ {missing_before - missing_after} valeurs manquantes comblées")
        
        # Étape 3: Standardisation des formats
        print("   3️⃣ Standardisation des formats...")
        df_improved = improver.standardize_format(df_improved, config['format_rules'])
        print(f"      ✅ {len(config['format_rules'])} colonnes formatées")
        
        # Étape 4: Normalisation de la codification (si applicable)
        if 'codification_rules' in config and config['codification_rules']:
            print("   4️⃣ Normalisation de la codification...")
            df_improved = improver.normalize_codification(df_improved, config['codification_rules'])
            print(f"      ✅ {len(config['codification_rules'])} colonnes codifiées")
        
        # Étape 5: Application des règles métier
        print("   5️⃣ Application des règles métier...")
        df_improved = improver.apply_business_rules(df_improved, config['business_rules'])
        print(f"      ✅ {len(config['business_rules'])} règles appliquées")
        
        # Résumé des améliorations
        improvement_summary = improver.get_improvement_summary()
        total_actions = sum(improvement_summary['by_category'].values())
        
        print(f"\n   📊 ACTIONS TOTALES: {total_actions}")
        for category, count in improvement_summary['by_category'].items():
            print(f"      • {category}: {count}")
        
        return df_improved

def main():
    """Fonction principale."""
    
    print("🎯 ETL QUALITÉ DES DONNÉES - SEUIL 99%")
    print("=" * 60)
    print(f"Objectif: Atteindre {QUALITY_THRESHOLD}% de qualité sur toutes les sources")
    print()
    
    # Vérifier les sources
    sources_dir = Path(SOURCES_PATH)
    if not sources_dir.exists():
        print(f"❌ Dossier '{SOURCES_PATH}' non trouvé!")
        return
    
    # Sources à traiter
    sources = ['population_paris', 'population_evry', 'consommation_paris', 'consommation_evry']
    available_sources = []
    
    for source in sources:
        file_path = sources_dir / f"{source}.csv"
        if file_path.exists():
            available_sources.append(source)
        else:
            print(f"⚠️  Source manquante: {source}")
    
    if not available_sources:
        print("❌ Aucune source de données trouvée!")
        return
    
    print(f"📋 {len(available_sources)} sources à traiter: {', '.join(available_sources)}")
    
    # Traiter chaque source
    etl = SimplifiedETL()
    results = []
    
    for source in available_sources:
        result = etl.process_source(source)
        if result:
            results.append(result)
    
    # Résumé global
    if results:
        print(f"\n📊 RÉSUMÉ GLOBAL")
        print("=" * 40)
        
        total_sources = len(results)
        conforming_sources = sum(1 for r in results if r['meets_threshold'])
        avg_improvement = sum(r['improvement'] for r in results) / total_sources
        total_rows_processed = sum(r['final_rows'] for r in results)
        
        print(f"   📋 Sources traitées: {total_sources}")
        print(f"   ✅ Sources conformes (≥{QUALITY_THRESHOLD}%): {conforming_sources}/{total_sources}")
        print(f"   📈 Amélioration moyenne: +{avg_improvement:.2f} points")
        print(f"   📊 Lignes traitées: {total_rows_processed:,}")
        
        print(f"\n   📊 DÉTAIL PAR SOURCE:")
        for result in results:
            status = "✅" if result['meets_threshold'] else "❌"
            print(f"      {status} {result['source_name']}: {result['score_before']:.1f}% → {result['score_after']:.1f}% (+{result['improvement']:.1f})")
        
        # Statut final
        print(f"\n🎯 STATUT FINAL")
        print("-" * 20)
        
        if conforming_sources == total_sources:
            print(f"   🎉 SUCCÈS COMPLET!")
            print(f"   ✅ Toutes les sources atteignent {QUALITY_THRESHOLD}%")
            print("   🏆 Objectif qualité atteint")
        else:
            non_conforming = total_sources - conforming_sources
            print(f"   ⚠️  SUCCÈS PARTIEL")
            print(f"   ❌ {non_conforming} source(s) sous le seuil")
            print("   🔧 Optimisations supplémentaires recommandées")
        
        print(f"\n📁 Fichiers créés dans '{OUTPUT_PATH}/':")
        for result in results:
            file_name = Path(result['file_saved']).name
            print(f"   📄 {file_name}")
    
    print(f"\n✅ ETL terminé!")

if __name__ == "__main__":
    main()