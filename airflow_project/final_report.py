#!/usr/bin/env python3
"""
🎉 RAPPORT FINAL - Résumé complet du projet ETL Qualité des Données
Analyse des résultats et recommandations d'amélioration
"""

import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

def analyze_final_results():
    """Analyser tous les résultats du projet."""
    
    print("🎉 RAPPORT FINAL - PROJET ETL QUALITÉ DES DONNÉES")
    print("=" * 65)
    print(f"📅 Date d'exécution: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print()
    
    # 1. Analyse des données sources
    print("📊 ANALYSE DES DONNÉES SOURCES")
    print("-" * 40)
    
    sources_dir = Path("sources")
    if sources_dir.exists():
        csv_files = list(sources_dir.glob("*.csv"))
        total_size = sum(f.stat().st_size for f in csv_files) / (1024**2)  # MB
        total_rows = 0
        
        print(f"   📁 Fichiers sources: {len(csv_files)}")
        
        for csv_file in sorted(csv_files):
            try:
                df = pd.read_csv(csv_file)
                rows = len(df)
                total_rows += rows
                size_mb = csv_file.stat().st_size / (1024**2)
                print(f"   📄 {csv_file.name:25} : {rows:7,} lignes ({size_mb:.1f} MB)")
            except:
                print(f"   ❌ {csv_file.name:25} : erreur lecture")
        
        print(f"   📊 TOTAL: {total_rows:,} lignes ({total_size:.1f} MB)")
    
    # 2. Analyse des résultats de qualité
    print(f"\n🔍 ANALYSE DES ÉVALUATIONS DE QUALITÉ")
    print("-" * 45)
    
    quality_dir = Path("quality_reports")
    if quality_dir.exists():
        # Rapports "before" et "after"
        before_reports = list(quality_dir.glob("*_before_quality.json"))
        after_reports = list(quality_dir.glob("*_after_quality.json"))
        
        print(f"   📋 Rapports avant amélioration: {len(before_reports)}")
        print(f"   📋 Rapports après amélioration: {len(after_reports)}")
        
        if before_reports and after_reports:
            print(f"\n   📊 COMPARAISON AVANT/APRÈS:")
            
            improvements = []
            
            for before_file in sorted(before_reports):
                source_name = before_file.name.replace('_before_quality.json', '')
                after_file = quality_dir / f"{source_name}_after_quality.json"
                
                if after_file.exists():
                    try:
                        with open(before_file, 'r') as f:
                            report_before = json.load(f)
                        with open(after_file, 'r') as f:
                            report_after = json.load(f)
                        
                        score_before = report_before['quality_score']['overall_quality_score']
                        score_after = report_after['quality_score']['overall_quality_score']
                        improvement = score_after - score_before
                        
                        status = "📈" if improvement > 0 else "📉" if improvement < 0 else "➡️"
                        
                        print(f"      {status} {source_name:20}: {score_before:5.1f}% → {score_after:5.1f}% ({improvement:+5.1f})")
                        
                        improvements.append({
                            'source': source_name,
                            'before': score_before,
                            'after': score_after,
                            'improvement': improvement
                        })
                        
                    except Exception as e:
                        print(f"      ❌ {source_name:20}: erreur lecture ({e})")
            
            if improvements:
                avg_improvement = sum(i['improvement'] for i in improvements) / len(improvements)
                positive_improvements = sum(1 for i in improvements if i['improvement'] > 0)
                
                print(f"\n   📈 Amélioration moyenne: {avg_improvement:+.2f} points")
                print(f"   ✅ Sources améliorées: {positive_improvements}/{len(improvements)}")
    
    # 3. Analyse des fichiers de sortie
    print(f"\n📁 ANALYSE DES FICHIERS DE SORTIE")
    print("-" * 40)
    
    output_dir = Path("output")
    if output_dir.exists():
        output_files = list(output_dir.glob("*.csv"))
        
        if output_files:
            total_output_size = 0
            total_output_rows = 0
            
            print(f"   📄 Fichiers créés: {len(output_files)}")
            
            for output_file in sorted(output_files):
                try:
                    df = pd.read_csv(output_file)
                    rows = len(df)
                    total_output_rows += rows
                    size_kb = output_file.stat().st_size / 1024
                    total_output_size += size_kb
                    
                    print(f"   📊 {output_file.name:30} : {rows:7,} lignes ({size_kb:6.1f} KB)")
                except:
                    print(f"   ❌ {output_file.name:30} : erreur lecture")
            
            print(f"   💾 TOTAL OUTPUT: {total_output_rows:,} lignes ({total_output_size:.1f} KB)")
        else:
            print("   ❌ Aucun fichier de sortie trouvé")
    
    # 4. Analyse des visualisations
    print(f"\n📈 ANALYSE DES VISUALISATIONS")
    print("-" * 35)
    
    viz_dir = Path("visualizations")
    if viz_dir.exists():
        png_files = list(viz_dir.glob("*.png"))
        html_files = list(viz_dir.glob("*.html"))
        
        if png_files:
            total_viz_size = sum(f.stat().st_size for f in png_files) / 1024  # KB
            print(f"   🖼️  Graphiques PNG: {len(png_files)} ({total_viz_size:.1f} KB)")
        
        if html_files:
            print(f"   🌐 Graphiques HTML: {len(html_files)}")
        
        if not png_files and not html_files:
            print("   ❌ Aucune visualisation trouvée")
    
    # 5. Évaluation par rapport au seuil de 99%
    print(f"\n🎯 ÉVALUATION SEUIL D'ACCEPTABILITÉ (99%)")
    print("-" * 50)
    
    quality_threshold = 99.0
    conforming_sources = 0
    total_sources = 0
    
    if quality_dir.exists():
        after_reports = list(quality_dir.glob("*_after_quality.json"))
        total_sources = len(after_reports)
        
        for report_file in after_reports:
            try:
                with open(report_file, 'r') as f:
                    report = json.load(f)
                
                source_name = report_file.name.replace('_after_quality.json', '')
                score = report['quality_score']['overall_quality_score']
                
                if score >= quality_threshold:
                    conforming_sources += 1
                    status = "✅ CONFORME"
                else:
                    gap = quality_threshold - score
                    status = f"❌ ÉCART: -{gap:.1f}pts"
                
                print(f"   {status:20} {source_name:20}: {score:5.1f}%")
                
            except:
                print(f"   ❌ ERREUR          {source_name:20}: lecture impossible")
    
    if total_sources > 0:
        conformity_rate = (conforming_sources / total_sources) * 100
        print(f"\n   📊 Taux de conformité: {conforming_sources}/{total_sources} ({conformity_rate:.1f}%)")
        
        if conforming_sources == total_sources:
            print("   🎉 EXCELLENT: Toutes les sources respectent le seuil!")
        elif conforming_sources > total_sources // 2:
            print("   ⚠️  ACCEPTABLE: Majorité des sources conformes")
        else:
            print("   🔧 INSUFFISANT: Améliorations majeures requises")
    
    # 6. Recommandations
    print(f"\n💡 RECOMMANDATIONS D'AMÉLIORATION")
    print("-" * 40)
    
    if conforming_sources < total_sources:
        print("   🔧 OPTIMISATIONS TECHNIQUES:")
        print("      • Revoir les règles de comblement des valeurs manquantes")
        print("      • Optimiser les stratégies de standardisation des formats")
        print("      • Ajuster les règles métier pour éviter la dégradation")
        print("      • Implémenter des validations plus strictes en amont")
        print()
    
    print("   📈 AMÉLIORATIONS FONCTIONNELLES:")
    print("      • Déployer Airflow avec Docker pour la production")
    print("      • Intégrer des alertes de qualité en temps réel")
    print("      • Automatiser la génération de rapports exécutifs")
    print("      • Implémenter un dashboard de monitoring")
    print()
    
    print("   🎯 OBJECTIFS À LONG TERME:")
    print("      • Atteindre 99% de qualité sur toutes les sources")
    print("      • Automatiser la détection d'anomalies")
    print("      • Intégrer d'autres sources de données énergétiques")
    print("      • Développer des modèles prédictifs de qualité")
    
    # 7. Bilan final
    print(f"\n🏆 BILAN FINAL DU PROJET")
    print("=" * 35)
    
    print("   ✅ RÉUSSITES:")
    print("      • Pipeline ETL complet et fonctionnel")
    print("      • Évaluation automatique de qualité des données")
    print("      • Amélioration intelligente des défauts détectés")
    print("      • Visualisations automatiques des métriques")
    print("      • Documentation complète et exécutable")
    print("      • Alternative locale fonctionnelle (sans Docker)")
    
    if conforming_sources < total_sources:
        print("\n   🔧 DÉFIS IDENTIFIÉS:")
        print("      • Seuil de 99% très exigeant pour données réelles")
        print("      • Certaines améliorations peuvent dégrader la qualité")
        print("      • Optimisation des règles métier nécessaire")
        print("      • Connectivité Docker à résoudre pour déploiement")
    
    print(f"\n   🎯 CONCLUSION:")
    if conforming_sources >= total_sources * 0.75:
        print("      🎉 PROJET RÉUSSI - Objectifs largement atteints")
    elif conforming_sources >= total_sources * 0.5:
        print("      ✅ PROJET SATISFAISANT - Objectifs partiellement atteints")
    else:
        print("      🔧 PROJET À OPTIMISER - Améliorations nécessaires")
    
    print(f"\n      📊 Score projet: {(conforming_sources/total_sources*100) if total_sources > 0 else 0:.1f}%")
    print("      💡 Base solide pour amélirations futures")

def main():
    """Générer le rapport final complet."""
    
    # Vérifier qu'on est dans le bon répertoire
    if not Path("sources").exists():
        print("❌ Erreur: Exécutez ce script depuis le répertoire du projet Airflow")
        return
    
    analyze_final_results()
    
    print(f"\n📋 FICHIERS À CONSULTER:")
    print("   📊 quality_reports/     - Rapports JSON détaillés")
    print("   📈 visualizations/      - Graphiques de qualité")
    print("   📁 output/              - Données améliorées")
    print("   📄 README.md            - Documentation mise à jour")
    
    print(f"\n🎉 Rapport final généré avec succès!")

if __name__ == "__main__":
    main()