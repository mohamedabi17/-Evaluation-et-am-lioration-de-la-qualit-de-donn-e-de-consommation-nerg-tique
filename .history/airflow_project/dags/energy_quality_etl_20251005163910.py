import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.quality_checks import check_completeness, check_duplicates

def extract_csv(src, **kwargs):
    df = pd.read_csv(src)
    kwargs['ti'].xcom_push(key=os.path.basename(src), value=df.to_json())

def evaluate_quality(src, **kwargs):
    df = pd.read_csv(src)
    completeness = check_completeness(df)
    duplicates = check_duplicates(df)
    print(f"Quality for {src}:\nCompleteness:\n{completeness}\nDuplicates: {duplicates}")

def clean_transform_population(city, **kwargs):
    src = f"/opt/airflow/data/population_{city}.csv"
    df = pd.read_csv(src)
    df = df.drop_duplicates()
    df = df.fillna({'Nom': 'Unknown', 'Prenom': 'Unknown', 'Adresse': 'Unknown', 'CSP': -1})
    df['CSP'] = df['CSP'].astype(str)
    df.to_csv(f"/opt/airflow/output/population_{city}_clean.csv", index=False)

def clean_transform_consommation(city, **kwargs):
    src = f"/opt/airflow/data/consommation_{city}.csv"
    df = pd.read_csv(src)
    df = df.drop_duplicates()
    df = df.fillna({'NB_KW_Jour': df['NB_KW_Jour'].mean()})
    df.to_csv(f"/opt/airflow/output/consommation_{city}_clean.csv", index=False)

def join_and_aggregate(city, **kwargs):
    pop = pd.read_csv(f"/opt/airflow/output/population_{city}_clean.csv")
    conso = pd.read_csv(f"/opt/airflow/output/consommation_{city}_clean.csv")
    iris = pd.read_csv("/opt/airflow/data/iris.csv")
    merged = pd.merge(pop, conso, left_on='Adresse', right_on='Nom_Rue', how='inner')
    merged = pd.merge(merged, iris, left_on='N', right_on='ID_Rue', how='inner')
    agg = merged.groupby('ID_IRIS')['NB_KW_Jour'].mean().reset_index()
    agg.rename(columns={'NB_KW_Jour': 'Conso_moyenne_annuelle'}, inplace=True)
    agg.to_csv(f"/opt/airflow/output/consommation_iris_{city}.csv", index=False)

def aggregate_csp(**kwargs):
    pop_paris = pd.read_csv("/opt/airflow/output/population_paris_clean.csv")
    pop_evry = pd.read_csv("/opt/airflow/output/population_evry_clean.csv")
    conso_paris = pd.read_csv("/opt/airflow/output/consommation_paris_clean.csv")
    conso_evry = pd.read_csv("/opt/airflow/output/consommation_evry_clean.csv")
    csp = pd.read_csv("/opt/airflow/data/csp.csv")
    pop = pd.concat([pop_paris, pop_evry])
    conso = pd.concat([conso_paris, conso_evry])
    merged = pd.merge(pop, conso, left_on='Adresse', right_on='Nom_Rue', how='inner')
    agg = merged.groupby('CSP')['NB_KW_Jour'].mean().reset_index()
    agg = pd.merge(agg, csp, left_on='CSP', right_on='ID_CSP', how='left')
    agg.rename(columns={'NB_KW_Jour': 'Conso_moyenne_annuelle'}, inplace=True)
    agg = agg[['ID_CSP', 'Conso_moyenne_annuelle', 'Salaire_Moyen']]
    agg.to_csv("/opt/airflow/output/consommation_csp.csv", index=False)

def load_targets(**kwargs):
    print("Data loaded to output folder.")

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'energy_quality_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

for city in ['paris', 'evry']:
    extract = PythonOperator(
        task_id=f'extract_{city}',
        python_callable=extract_csv,
        op_kwargs={'src': f'/opt/airflow/data/population_{city}.csv'},
        dag=dag
    )
    quality = PythonOperator(
        task_id=f'evaluate_quality_{city}',
        python_callable=evaluate_quality,
        op_kwargs={'src': f'/opt/airflow/data/population_{city}.csv'},
        dag=dag
    )
    clean_pop = PythonOperator(
        task_id=f'clean_transform_population_{city}',
        python_callable=clean_transform_population,
        op_kwargs={'city': city},
        dag=dag
    )
    clean_conso = PythonOperator(
        task_id=f'clean_transform_consommation_{city}',
        python_callable=clean_transform_consommation,
        op_kwargs={'city': city},
        dag=dag
    )
    join_agg = PythonOperator(
        task_id=f'join_and_aggregate_{city}',
        python_callable=join_and_aggregate,
        op_kwargs={'city': city},
        dag=dag
    )
    extract >> quality >> clean_pop >> clean_conso >> join_agg

agg_csp = PythonOperator(
    task_id='aggregate_csp',
    python_callable=aggregate_csp,
    dag=dag
)

load = PythonOperator(
    task_id='load_targets',
    python_callable=load_targets,
    dag=dag
)

[join_agg for join_agg in dag.tasks if 'join_and_aggregate' in join_agg.task_id] >> agg_csp >> load
