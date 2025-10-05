# 🚀 GUIDE COMPLET POUR LANCER AIRFLOW SUR LOCALHOST

## 🎯 OBJECTIF
Démarrer Apache Airflow sur http://localhost:8080 pour visualiser et exécuter votre DAG de qualité des données.

## 📋 MÉTHODE 1: SCRIPT AUTOMATIQUE (Recommandée)
```bash
python start_airflow.py
```
Puis ouvrez: http://localhost:8080 (admin/admin)

## 📋 MÉTHODE 2: INSTALLATION MANUELLE

### Étape 1: Installation
```bash
pip install apache-airflow
```

### Étape 2: Configuration
```bash
# Windows
set AIRFLOW_HOME=%USERPROFILE%\airflow

# Linux/Mac  
export AIRFLOW_HOME=~/airflow
```

### Étape 3: Initialisation
```bash
airflow db init
```

### Étape 4: Créer utilisateur admin
```bash
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### Étape 5: Démarrer Airflow
```bash
airflow standalone
```

## 📋 MÉTHODE 3: DEUX TERMINAUX SÉPARÉS

### Terminal 1: Webserver
```bash
airflow webserver --port 8080
```

### Terminal 2: Scheduler  
```bash
airflow scheduler
```

## 🔧 INSTALLATION DE VOTRE DAG

### 1. Localiser le dossier DAGs
- Windows: `%USERPROFILE%\airflow\dags\`
- Linux/Mac: `~/airflow/dags/`

### 2. Copier votre DAG
```bash
# Copier le fichier principal
copy dags\energy_quality_etl.py %USERPROFILE%\airflow\dags\

# Copier les utilitaires
xcopy utils %USERPROFILE%\airflow\dags\utils\ /E /I

# Copier les données sources
xcopy sources %USERPROFILE%\airflow\dags\sources\ /E /I
```

### 3. Actualiser l'interface Airflow
- Allez sur http://localhost:8080
- Votre DAG "energy_quality_etl" devrait apparaître
- Activez-le avec le toggle switch

## 🌐 ACCÈS À L'INTERFACE

- **URL**: http://localhost:8080
- **Utilisateur**: admin  
- **Mot de passe**: admin

## 📊 UTILISATION DE VOTRE DAG

1. **Localiser le DAG**: Cherchez "energy_quality_etl" dans la liste
2. **Activer**: Cliquez sur le toggle pour l'activer
3. **Exécuter**: Cliquez sur le bouton "Trigger DAG"
4. **Suivre**: Cliquez sur le DAG pour voir les détails d'exécution
5. **Logs**: Cliquez sur chaque tâche pour voir les logs détaillés

## 🔧 DÉPANNAGE

### Problème: Airflow ne s'installe pas
```bash
# Essayer avec une version spécifique
pip install apache-airflow==2.7.3

# Ou sans dépendances
pip install apache-airflow --no-deps
```

### Problème: Port 8080 déjà utilisé
```bash
# Utiliser un autre port
airflow webserver --port 8081
```

### Problème: DAG n'apparaît pas
- Vérifiez que les fichiers sont dans `airflow/dags/`
- Vérifiez les logs d'erreur dans l'interface
- Redémarrez Airflow

### Problème: Erreurs d'import Python
- Assurez-vous que `utils/` est copié dans `airflow/dags/`
- Vérifiez que pandas, numpy, etc. sont installés

## 🚀 COMMANDES RAPIDES

```bash
# Démarrage complet
python start_airflow.py

# Vérifier le statut
airflow dags list

# Tester un DAG
airflow dags test energy_quality_etl 2025-10-05

# Voir les logs
airflow logs energy_quality_etl extract_source_data 2025-10-05
```

## ✅ VÉRIFICATION DU SUCCÈS

1. ✅ Interface accessible sur http://localhost:8080
2. ✅ Connexion réussie avec admin/admin  
3. ✅ DAG "energy_quality_etl" visible et activé
4. ✅ Exécution manuelle réussie
5. ✅ Logs détaillés des tâches visibles

## 🎉 RÉSULTAT ATTENDU

Une fois Airflow lancé, vous pourrez:
- Visualiser graphiquement votre pipeline ETL
- Exécuter les tâches individuellement ou en chaîne
- Consulter les logs détaillés de chaque étape
- Programmer des exécutions automatiques
- Monitorer les performances et erreurs

Votre DAG de qualité des données sera entièrement opérationnel dans l'interface web d'Airflow !