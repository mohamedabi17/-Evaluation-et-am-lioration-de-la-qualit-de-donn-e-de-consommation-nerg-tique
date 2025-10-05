import os
import pandas as pd
import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')
from utils.quality_checks import (
    generate_quality_report, 
    create_quality_visualizations,
    check_completeness,
    check_duplicates,
    check_codification_consistency
)

# Configuration des chemins
SOURCES_PATH = "/opt/airflow/sources"
OUTPUT_PATH = "/opt/airflow/output"
QUALITY_REPORTS_PATH = "/opt/airflow/quality_reports"
VISUALIZATIONS_PATH = "/opt/airflow/visualizations"

def extract_source_data(source_name, **kwargs):
    """Extraire les données sources et les stocker dans XCom."""
    file_path = f"{SOURCES_PATH}/{source_name}.csv"
    df = pd.read_csv(file_path)
    
    # Stocker les statistiques basiques
    stats = {
        'rows': len(df),
        'columns': len(df.columns),
        'file_size_kb': round(os.path.getsize(file_path) / 1024, 2)
    }
    
    print(f" Extraction de {source_name}: {stats['rows']} lignes, {stats['columns']} colonnes")
    return stats

def evaluate_data_quality(source_name, **kwargs):
    """Évaluer la qualité des données et générer un rapport complet."""
    file_path = f"{SOURCES_PATH}/{source_name}.csv"
    df = pd.read_csv(file_path)
    
    # Générer le rapport de qualité
    report_path = f"{QUALITY_REPORTS_PATH}/{source_name}_quality_report.json"
    quality_report = generate_quality_report(df, source_name, report_path)
    
    # Créer les visualisations
    create_quality_visualizations(quality_report, VISUALIZATIONS_PATH)
    
    # Log des métriques importantes
    quality_score = quality_report['quality_score']
    print(f"   Qualité de {source_name}:")
    print(f"   Score global: {quality_score['overall_quality_score']}% ({quality_score['quality_level']})")
    print(f"   Complétude: {quality_score['completeness_score']}%")
    print(f"   Unicité: {quality_score['uniqueness_score']}%")
    print(f"   Format: {quality_score['format_score']}%")
    print(f"   Codification: {quality_score['codification_score']}%")
    
    # Identifier les problèmes critiques
    critical_issues = []
    if quality_score['overall_quality_score'] < 70:
        critical_issues.append("Score global faible")
    if quality_score['completeness_score'] < 80:
        critical_issues.append("Problèmes de complétude")
    if quality_score['uniqueness_score'] < 90:
        critical_issues.append("Présence de doublons")
    if quality_score['codification_score'] < 90:
        critical_issues.append("Problèmes de codification")
    
    if critical_issues:
        print(f" Problèmes identifiés: {', '.join(critical_issues)}")
    
    return {
        'quality_score': quality_score['overall_quality_score'],
        'critical_issues': critical_issues,
        'needs_cleaning': quality_score['overall_quality_score'] < 80
    }
#need
def decide_cleaning_strategy(**kwargs):
    """Décider de la stratégie de nettoyage basée sur l'évaluation qualité."""
    ti = kwargs['ti']
    
    # Récupérer les résultats d'évaluation pour toutes les sources
    sources = ['population_paris', 'population_evry', 'consommation_paris', 'consommation_evry']
    overall_needs_cleaning = False
    
    for source in sources:
        result = ti.xcom_pull(task_ids=f'evaluate_quality_{source}')
        if result and result.get('needs_cleaning', False):
            overall_needs_cleaning = True
            break
    
    if overall_needs_cleaning:
        print(" Nettoyage nécessaire - Procédure d'amélioration activée")
        return 'improve_data_quality'
    else:
        print(" Qualité suffisante - Passage direct à l'intégration")
        return 'integrate_data'

def improve_data_quality(**kwargs):
    """Améliorer la qualité des données selon les critères identifiés."""
    
    # Traitement Population Paris
    pop_paris = pd.read_csv(f"{SOURCES_PATH}/population_paris.csv")
    pop_paris_clean = clean_population_data(pop_paris, "Paris")
    pop_paris_clean.to_csv(f"{OUTPUT_PATH}/population_paris_clean.csv", index=False)
    
    # Traitement Population Évry
    pop_evry = pd.read_csv(f"{SOURCES_PATH}/population_evry.csv")
    pop_evry_clean = clean_population_data(pop_evry, "Évry")
    pop_evry_clean.to_csv(f"{OUTPUT_PATH}/population_evry_clean.csv", index=False)
    
    # Traitement Consommation Paris
    conso_paris = pd.read_csv(f"{SOURCES_PATH}/consommation_paris.csv")
    conso_paris_clean = clean_consommation_data(conso_paris, "Paris")
    conso_paris_clean.to_csv(f"{OUTPUT_PATH}/consommation_paris_clean.csv", index=False)
    
    # Traitement Consommation Évry
    conso_evry = pd.read_csv(f"{SOURCES_PATH}/consommation_evry.csv")
    conso_evry_clean = clean_consommation_data(conso_evry, "Évry")
    conso_evry_clean.to_csv(f"{OUTPUT_PATH}/consommation_evry_clean.csv", index=False)
    
    print("🧹 Nettoyage des données terminé")
    
    # Générer un rapport post-nettoyage
    generate_post_cleaning_report()

def clean_population_data(df, city):
    """Nettoyer les données de population selon les critères de qualité."""
    original_rows = len(df)
    
    # 1. Élimination des doublons
    df = df.drop_duplicates()
    duplicates_removed = original_rows - len(df)
    
    # 2. Traitement des valeurs manquantes
    df['Nom'] = df['Nom'].fillna('INCONNU')
    df['Prenom'] = df['Prenom'].fillna('INCONNU')
    df['Adresse'] = df['Adresse'].fillna('ADRESSE_INCONNUE')
    
    # 3. Normalisation des codes CSP (Conformité à la codification)
    csp_mapping = {
        'cadre': '1',
        'employe': '2', 
        'employé': '2',
        'ouvrier': '3',
        'retraite': '4',
        'retraité': '4'
    }
    
    # Conversion des CSP textuels en codes numériques
    df['CSP'] = df['CSP'].astype(str).str.lower()
    df['CSP'] = df['CSP'].replace(csp_mapping)
    
    # Validation des codes CSP
    valid_csp = ['1', '2', '3', '4']
    df['CSP'] = df['CSP'].where(df['CSP'].isin(valid_csp), '0')  # 0 = Indéterminé
    
    # 4. Standardisation des adresses (Format)
    df['Adresse'] = df['Adresse'].str.title()
    
    print(f"🧹 {city} Population - Doublons supprimés: {duplicates_removed}")
    
    return df

def clean_consommation_data(df, city):
    """Nettoyer les données de consommation."""
    original_rows = len(df)
    
    # 1. Élimination des doublons
    df = df.drop_duplicates()
    duplicates_removed = original_rows - len(df)
    
    # 2. Traitement des valeurs manquantes dans NB_KW_Jour
    mean_consumption = df['NB_KW_Jour'].mean()
    df['NB_KW_Jour'] = df['NB_KW_Jour'].fillna(mean_consumption)
    
    # 3. Traitement des numéros de rue manquants
    df['N'] = df['N'].fillna(0)
    
    # 4. Standardisation des noms de rue
    df['Nom_Rue'] = df['Nom_Rue'].fillna('RUE_INCONNUE')
    df['Nom_Rue'] = df['Nom_Rue'].str.title()
    
    # 5. Validation des codes postaux
    if city == "Paris":
        df['Code_Postal'] = df['Code_Postal'].where(
            df['Code_Postal'].astype(str).str.startswith('75'), '75000'
        )
    elif city == "Évry":
        df['Code_Postal'] = df['Code_Postal'].where(
            df['Code_Postal'].astype(str).str.startswith('91'), '91000'
        )
    
    print(f"🧹 {city} Consommation - Doublons supprimés: {duplicates_removed}")
    
    return df

def generate_post_cleaning_report():
    """Générer un rapport de qualité post-nettoyage."""
    sources_clean = {
        'population_paris': f"{OUTPUT_PATH}/population_paris_clean.csv",
        'population_evry': f"{OUTPUT_PATH}/population_evry_clean.csv", 
        'consommation_paris': f"{OUTPUT_PATH}/consommation_paris_clean.csv",
        'consommation_evry': f"{OUTPUT_PATH}/consommation_evry_clean.csv"
    }
    
    improvement_report = {
        'timestamp': datetime.now().isoformat(),
        'sources_improved': {}
    }
    
    for source_name, file_path in sources_clean.items():
        if os.path.exists(file_path):
            df_clean = pd.read_csv(file_path)
            quality_report = generate_quality_report(df_clean, f"{source_name}_clean")
            improvement_report['sources_improved'][source_name] = quality_report['quality_score']
    
    # Sauvegarder le rapport d'amélioration
    with open(f"{QUALITY_REPORTS_PATH}/improvement_report.json", 'w', encoding='utf-8') as f:
        json.dump(improvement_report, f, indent=2, ensure_ascii=False)

def integrate_data(**kwargs):
    """Intégrer les données pour créer les tables cibles."""
    
    # Charger les données nettoyées ou originales
    def load_data(source, cleaned=True):
        if cleaned and os.path.exists(f"{OUTPUT_PATH}/{source}_clean.csv"):
            return pd.read_csv(f"{OUTPUT_PATH}/{source}_clean.csv")
        else:
            return pd.read_csv(f"{SOURCES_PATH}/{source}.csv")
    
    # Charger toutes les données
    pop_paris = load_data('population_paris')
    pop_evry = load_data('population_evry')
    conso_paris = load_data('consommation_paris')
    conso_evry = load_data('consommation_evry')
    csp = pd.read_csv(f"{SOURCES_PATH}/csp.csv")
    iris = pd.read_csv(f"{SOURCES_PATH}/iris.csv")
    
    # 1. Créer Consommation_IRIS_Paris
    create_consommation_iris_table(pop_paris, conso_paris, iris, "paris")
    
    # 2. Créer Consommation_IRIS_Evry  
    create_consommation_iris_table(pop_evry, conso_evry, iris, "evry")
    
    # 3. Créer Consommation_CSP
    create_consommation_csp_table(pop_paris, pop_evry, conso_paris, conso_evry, csp)
    
    print("🔗 Intégration des données terminée")

def create_consommation_iris_table(population, consommation, iris, city):
    """Créer la table Consommation_IRIS pour une ville."""
    
    # Jointure Population + Consommation sur l'adresse
    # Simplification: on joint sur les ID (dans un vrai cas, on ferait un matching d'adresse)
    merged = pd.merge(
        population[['ID_Personne', 'Adresse']], 
        consommation[['ID_Adr', 'Nom_Rue', 'NB_KW_Jour']], 
        left_on='ID_Personne', 
        right_on='ID_Adr', 
        how='inner'
    )
    
    # Jointure avec IRIS sur ID_Rue (simplifié)
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
    
    # Conversion en consommation annuelle (approximation)
    result['Conso_moyenne_annuelle'] = (result['Conso_moyenne_annuelle'] * 365).round(2)
    
    output_file = f"{OUTPUT_PATH}/consommation_iris_{city}.csv"
    result.to_csv(output_file, index=False)
    
    print(f"📊 Table Consommation_IRIS_{city.title()} créée: {len(result)} IRIS")

def create_consommation_csp_table(pop_paris, pop_evry, conso_paris, conso_evry, csp):
    """Créer la table Consommation_CSP globale."""
    
    # Combiner toutes les données de population et consommation
    pop_all = pd.concat([pop_paris, pop_evry])
    conso_all = pd.concat([conso_paris, conso_evry])
    
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
    
    # Jointure avec la table CSP pour les salaires
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
    
    output_file = f"{OUTPUT_PATH}/consommation_csp.csv"
    result.to_csv(output_file, index=False)
    
    print(f"📊 Table Consommation_CSP créée: {len(result)} catégories")

def generate_final_report(**kwargs):
    """Générer un rapport final de l'ETL."""
    
    final_report = {
        'timestamp': datetime.now().isoformat(),
        'etl_status': 'completed',
        'output_files': []
    }
    
    # Vérifier les fichiers de sortie
    output_files = [
        'consommation_iris_paris.csv',
        'consommation_iris_evry.csv', 
        'consommation_csp.csv'
    ]
    
    for file in output_files:
        file_path = f"{OUTPUT_PATH}/{file}"
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            final_report['output_files'].append({
                'file': file,
                'rows': len(df),
                'size_kb': round(os.path.getsize(file_path) / 1024, 2)
            })
    
    # Sauvegarder le rapport final
    with open(f"{QUALITY_REPORTS_PATH}/final_etl_report.json", 'w', encoding='utf-8') as f:
        json.dump(final_report, f, indent=2, ensure_ascii=False)
    
    print("📋 Rapport final généré")
    print(f"✅ ETL terminé avec succès - {len(final_report['output_files'])} fichiers créés")

# Configuration du DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'energy_data_quality_etl',
    default_args=default_args,
    description='ETL avec évaluation et amélioration de la qualité des données énergétiques',
    schedule_interval=None,
    catchup=False,
    tags=['data_quality', 'etl', 'energy']
)

# Création des dossiers de sortie
create_dirs = BashOperator(
    task_id='create_output_directories',
    bash_command=f'mkdir -p {OUTPUT_PATH} {QUALITY_REPORTS_PATH} {VISUALIZATIONS_PATH}',
    dag=dag
)

# Tâches d'extraction
sources = ['population_paris', 'population_evry', 'consommation_paris', 'consommation_evry']
extract_tasks = []
quality_tasks = []

for source in sources:
    # Extraction
    extract_task = PythonOperator(
        task_id=f'extract_{source}',
        python_callable=extract_source_data,
        op_kwargs={'source_name': source},
        dag=dag
    )
    extract_tasks.append(extract_task)
    
    # Évaluation qualité
    quality_task = PythonOperator(
        task_id=f'evaluate_quality_{source}',
        python_callable=evaluate_data_quality,
        op_kwargs={'source_name': source},
        dag=dag
    )
    quality_tasks.append(quality_task)
    
    # Dépendances
    create_dirs >> extract_task >> quality_task

# Décision sur la stratégie de nettoyage
decide_strategy = BranchPythonOperator(
    task_id='decide_cleaning_strategy',
    python_callable=decide_cleaning_strategy,
    dag=dag
)

# Amélioration de la qualité
improve_quality = PythonOperator(
    task_id='improve_data_quality',
    python_callable=improve_data_quality,
    dag=dag
)

# Intégration des données
integrate = PythonOperator(
    task_id='integrate_data',
    python_callable=integrate_data,
    dag=dag
)

# Rapport final
final_report = PythonOperator(
    task_id='generate_final_report',
    python_callable=generate_final_report,
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

# Définition des dépendances
quality_tasks >> decide_strategy
decide_strategy >> improve_quality >> integrate >> final_report
decide_strategy >> integrate
