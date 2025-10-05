"""
📊 VISUALISEUR DE RÉSULTATS ETL
Affiche en temps réel les résultats du pipeline de qualité des données
"""

import os
import json
import pandas as pd
from pathlib import Path
import time

def display_quality_results():
    """Afficher les résultats des évaluations de qualité."""
    
    print("📊 RÉSULTATS DE L'ÉVALUATION DE QUALITÉ")
    print("=" * 55)
    
    quality_dir = Path("quality_reports")
    
    if not quality_dir.exists():
        print("❌ Aucun rapport de qualité trouvé")
        return
    
    # Lister tous les rapports JSON
    reports = list(quality_dir.glob("*_quality_report.json"))
    
    if not reports:
        print("❌ Aucun rapport de qualité trouvé")
        return
    
    print(f"📋 {len(reports)} rapports de qualité trouvés\n")
    
    for report_file in sorted(reports):
        try:
            with open(report_file, 'r', encoding='utf-8') as f:
                report = json.load(f)
            
            source_name = report['source_name']
            dataset_info = report['dataset_info']
            quality_score = report['quality_score']
            
            print(f"📄 {source_name.upper()}")
            print("-" * 30)
            print(f"   📊 Données: {dataset_info['total_rows']:,} lignes, {dataset_info['total_columns']} colonnes")
            print(f"   🎯 Score global: {quality_score['overall_quality_score']:.1f}% ({quality_score['quality_level']})")
            print(f"   ✅ Complétude: {quality_score['completeness_score']:.1f}%")
            print(f"   🔄 Unicité: {quality_score['uniqueness_score']:.1f}%")
            
            if 'format_consistency_score' in quality_score:
                print(f"   📝 Format: {quality_score['format_consistency_score']:.1f}%")
            
            print()
            
        except Exception as e:
            print(f"❌ Erreur lecture {report_file}: {e}")

def display_output_files():
    """Afficher les fichiers de sortie créés."""
    
    print("📁 FICHIERS DE SORTIE CRÉÉS")
    print("=" * 40)
    
    output_dir = Path("output")
    
    if not output_dir.exists():
        print("❌ Dossier output non trouvé")
        return
    
    csv_files = list(output_dir.glob("*.csv"))
    
    if not csv_files:
        print("❌ Aucun fichier de sortie trouvé")
        return
    
    total_size = 0
    
    for csv_file in sorted(csv_files):
        size_kb = csv_file.stat().st_size / 1024
        total_size += size_kb
        
        # Lire quelques lignes pour avoir un aperçu
        try:
            df = pd.read_csv(csv_file)
            rows = len(df)
            cols = len(df.columns)
            
            print(f"📄 {csv_file.name}")
            print(f"   📊 {rows:,} lignes × {cols} colonnes")
            print(f"   💾 {size_kb:.1f} KB")
            
            # Aperçu des colonnes
            if len(df.columns) <= 6:
                print(f"   📋 Colonnes: {', '.join(df.columns)}")
            else:
                cols_preview = ', '.join(df.columns[:3]) + f" ... (+{len(df.columns)-3} autres)"
                print(f"   📋 Colonnes: {cols_preview}")
            
            print()
            
        except Exception as e:
            print(f"   ❌ Erreur lecture: {e}")
            print()
    
    print(f"💾 TOTAL: {total_size:.1f} KB ({len(csv_files)} fichiers)")

def display_visualizations():
    """Afficher les visualisations créées."""
    
    print("📈 VISUALISATIONS CRÉÉES")
    print("=" * 35)
    
    viz_dir = Path("visualizations")
    
    if not viz_dir.exists():
        print("❌ Dossier visualizations non trouvé")
        return
    
    # Chercher les fichiers HTML et PNG
    html_files = list(viz_dir.glob("*.html"))
    png_files = list(viz_dir.glob("*.png"))
    
    if not html_files and not png_files:
        print("❌ Aucune visualisation trouvée")
        return
    
    if html_files:
        print(f"🌐 {len(html_files)} graphiques interactifs (HTML):")
        for html_file in sorted(html_files):
            print(f"   📊 {html_file.name}")
        print()
    
    if png_files:
        print(f"🖼️  {len(png_files)} graphiques statiques (PNG):")
        for png_file in sorted(png_files):
            size_kb = png_file.stat().st_size / 1024
            print(f"   📊 {png_file.name} ({size_kb:.1f} KB)")

def main():
    """Afficher tous les résultats disponibles."""
    
    print("🔍 ANALYSE DES RÉSULTATS DU PIPELINE ETL")
    print("=" * 60)
    print()
    
    # Vérifier le répertoire de travail
    project_dir = Path(".")
    
    if not any(project_dir.glob("sources/*.csv")):
        print("❌ Vous n'êtes pas dans le bon répertoire")
        print("💡 Naviguez vers le dossier du projet Airflow")
        return
    
    # 1. Résultats de qualité
    display_quality_results()
    print()
    
    # 2. Fichiers de sortie
    display_output_files()
    print()
    
    # 3. Visualisations
    display_visualizations()
    print()
    
    # 4. Recommandations
    print("💡 ACTIONS RECOMMANDÉES")
    print("=" * 30)
    print("1. Ouvrez les fichiers CSV dans output/ pour voir les données finales")
    print("2. Consultez les rapports JSON dans quality_reports/")
    print("3. Visualisez les graphiques dans visualizations/")
    print("4. Pour Docker: résolvez d'abord les problèmes de connectivité")
    print()
    
    # 5. Proposer d'ouvrir les fichiers
    try:
        import webbrowser
        
        viz_dir = Path("visualizations")
        html_files = list(viz_dir.glob("*.html"))
        
        if html_files:
            print("🌐 Ouvrir les visualisations dans le navigateur ? (y/n): ", end="")
            response = input().strip().lower()
            
            if response == 'y':
                for html_file in html_files[:3]:  # Ouvrir max 3 fichiers
                    file_path = f"file://{html_file.absolute()}"
                    webbrowser.open(file_path)
                    print(f"   🌐 Ouvert: {html_file.name}")
                
                if len(html_files) > 3:
                    print(f"   ⚠️  {len(html_files)-3} autres fichiers disponibles")
    
    except KeyboardInterrupt:
        print("\n👋 Analyse interrompue")
    except Exception as e:
        print(f"❌ Erreur ouverture navigateur: {e}")

if __name__ == "__main__":
    main()