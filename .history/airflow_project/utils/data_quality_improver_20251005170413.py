"""
Utilitaires pour l'amélioration de la qualité des données
Ce module contient des fonctions spécialisées pour corriger les problèmes de qualité identifiés
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import re

class DataQualityImprover:
    """Classe pour améliorer la qualité des données selon différents critères."""
    
    def __init__(self):
        self.improvement_log = []
    
    def log_improvement(self, action: str, details: str):
        """Enregistrer les améliorations apportées."""
        self.improvement_log.append({
            'action': action,
            'details': details,
            'timestamp': pd.Timestamp.now()
        })
    
    def improve_completeness(self, df: pd.DataFrame, strategies: Dict) -> pd.DataFrame:
        """
        Améliorer la complétude des données.
        
        strategies: dict avec clé=colonne, valeur=stratégie ('mean', 'mode', 'forward_fill', 'custom_value')
        """
        df_improved = df.copy()
        
        for column, strategy in strategies.items():
            if column not in df_improved.columns:
                continue
                
            missing_count = df_improved[column].isnull().sum()
            if missing_count == 0:
                continue
            
            if strategy == 'mean':
                fill_value = df_improved[column].mean()
                df_improved[column].fillna(fill_value, inplace=True)
                
            elif strategy == 'mode':
                fill_value = df_improved[column].mode().iloc[0] if len(df_improved[column].mode()) > 0 else 'UNKNOWN'
                df_improved[column].fillna(fill_value, inplace=True)
                
            elif strategy == 'forward_fill':
                df_improved[column].fillna(method='ffill', inplace=True)
                
            elif isinstance(strategy, str) and strategy.startswith('custom_'):
                fill_value = strategy.replace('custom_', '')
                df_improved[column].fillna(fill_value, inplace=True)
            
            self.log_improvement(
                'completeness',
                f"Colonne {column}: {missing_count} valeurs manquantes comblées avec stratégie '{strategy}'"
            )
        
        return df_improved
    
    def remove_duplicates(self, df: pd.DataFrame, subset: List[str] = None, keep: str = 'first') -> pd.DataFrame:
        """Supprimer les doublons avec logging détaillé."""
        original_count = len(df)
        df_improved = df.drop_duplicates(subset=subset, keep=keep)
        duplicates_removed = original_count - len(df_improved)
        
        if duplicates_removed > 0:
            self.log_improvement(
                'duplicates',
                f"{duplicates_removed} doublons supprimés (subset={subset}, keep={keep})"
            )
        
        return df_improved
    
    def standardize_format(self, df: pd.DataFrame, format_rules: Dict) -> pd.DataFrame:
        """
        Standardiser les formats selon des règles prédéfinies.
        
        format_rules: dict avec clé=colonne, valeur=règle de formatage
        """
        df_improved = df.copy()
        
        for column, rule in format_rules.items():
            if column not in df_improved.columns:
                continue
            
            if rule == 'title_case':
                df_improved[column] = df_improved[column].astype(str).str.title()
                
            elif rule == 'upper_case':
                df_improved[column] = df_improved[column].astype(str).str.upper()
                
            elif rule == 'lower_case':
                df_improved[column] = df_improved[column].astype(str).str.lower()
                
            elif rule == 'numeric':
                df_improved[column] = pd.to_numeric(df_improved[column], errors='coerce')
                
            elif rule == 'clean_spaces':
                df_improved[column] = df_improved[column].astype(str).str.strip()
                
            elif isinstance(rule, dict) and 'regex' in rule:
                # Règle de regex personnalisée
                pattern = rule['regex']
                replacement = rule.get('replacement', '')
                df_improved[column] = df_improved[column].astype(str).str.replace(pattern, replacement, regex=True)
            
            self.log_improvement(
                'format',
                f"Colonne {column}: format standardisé avec règle '{rule}'"
            )
        
        return df_improved
    
    def normalize_codification(self, df: pd.DataFrame, codification_rules: Dict) -> pd.DataFrame:
        """
        Normaliser les codes selon des règles de mapping.
        
        codification_rules: dict avec clé=colonne, valeur=dict de mapping
        """
        df_improved = df.copy()
        
        for column, mapping in codification_rules.items():
            if column not in df_improved.columns:
                continue
            
            # Appliquer le mapping
            original_values = df_improved[column].unique()
            df_improved[column] = df_improved[column].astype(str).replace(mapping)
            
            # Identifier les valeurs non mappées
            unmapped = set(original_values) - set(mapping.keys())
            if unmapped:
                self.log_improvement(
                    'codification',
                    f"Colonne {column}: {len(mapping)} codes mappés, {len(unmapped)} valeurs non mappées: {unmapped}"
                )
            else:
                self.log_improvement(
                    'codification',
                    f"Colonne {column}: {len(mapping)} codes normalisés avec succès"
                )
        
        return df_improved
    
    def handle_granularity_issues(self, df: pd.DataFrame, aggregation_rules: Dict) -> pd.DataFrame:
        """
        Gérer les problèmes d'hétérogénéité de granularité.
        
        aggregation_rules: dict avec règles d'agrégation par groupe
        """
        df_improved = df.copy()
        
        for rule_name, rule_config in aggregation_rules.items():
            group_by = rule_config.get('group_by', [])
            agg_column = rule_config.get('agg_column')
            agg_method = rule_config.get('method', 'mean')
            
            if not all(col in df_improved.columns for col in group_by + [agg_column]):
                continue
            
            # Effectuer l'agrégation
            if agg_method == 'mean':
                aggregated = df_improved.groupby(group_by)[agg_column].mean().reset_index()
            elif agg_method == 'sum':
                aggregated = df_improved.groupby(group_by)[agg_column].sum().reset_index()
            elif agg_method == 'count':
                aggregated = df_improved.groupby(group_by)[agg_column].count().reset_index()
            
            self.log_improvement(
                'granularity',
                f"Règle {rule_name}: agrégation {agg_method} sur {agg_column} par {group_by}"
            )
        
        return df_improved
    
    def apply_business_rules(self, df: pd.DataFrame, business_rules: List[Dict]) -> pd.DataFrame:
        """
        Appliquer des règles métier personnalisées.
        
        business_rules: liste de règles avec conditions et actions
        """
        df_improved = df.copy()
        
        for rule in business_rules:
            rule_name = rule.get('name', 'unnamed_rule')
            condition = rule.get('condition')
            action = rule.get('action')
            
            try:
                # Appliquer la condition
                mask = df_improved.eval(condition)
                affected_rows = mask.sum()
                
                if affected_rows > 0:
                    # Appliquer l'action
                    if action['type'] == 'set_value':
                        df_improved.loc[mask, action['column']] = action['value']
                    elif action['type'] == 'multiply':
                        df_improved.loc[mask, action['column']] *= action['factor']
                    elif action['type'] == 'flag':
                        df_improved.loc[mask, action['flag_column']] = action['flag_value']
                    
                    self.log_improvement(
                        'business_rule',
                        f"Règle {rule_name}: {affected_rows} lignes affectées"
                    )
                        
            except Exception as e:
                self.log_improvement(
                    'business_rule_error',
                    f"Erreur dans la règle {rule_name}: {str(e)}"
                )
        
        return df_improved
    
    def get_improvement_summary(self) -> Dict:
        """Obtenir un résumé des améliorations apportées."""
        summary = {
            'total_improvements': len(self.improvement_log),
            'by_category': {},
            'timeline': self.improvement_log
        }
        
        for log_entry in self.improvement_log:
            category = log_entry['action']
            if category not in summary['by_category']:
                summary['by_category'][category] = 0
            summary['by_category'][category] += 1
        
        return summary

def create_improvement_config_population():
    """Configuration d'amélioration pour les données de population."""
    return {
        'completeness_strategies': {
            'Nom': 'custom_INCONNU',
            'Prenom': 'custom_INCONNU', 
            'Adresse': 'custom_ADRESSE_INCONNUE',
            'CSP': 'custom_0'
        },
        'format_rules': {
            'Nom': 'title_case',
            'Prenom': 'title_case',
            'Adresse': 'title_case',
            'CSP': 'clean_spaces'
        },
        'codification_rules': {
            'CSP': {
                'cadre': '1',
                'employe': '2',
                'employé': '2', 
                'ouvrier': '3',
                'retraite': '4',
                'retraité': '4'
            }
        },
        'business_rules': [
            {
                'name': 'validate_csp_codes',
                'condition': "~CSP.isin(['1', '2', '3', '4'])",
                'action': {
                    'type': 'set_value',
                    'column': 'CSP',
                    'value': '0'
                }
            }
        ]
    }

def create_improvement_config_consommation():
    """Configuration d'amélioration pour les données de consommation."""
    return {
        'completeness_strategies': {
            'N': 'custom_0',
            'Nom_Rue': 'custom_RUE_INCONNUE',
            'NB_KW_Jour': 'mean'
        },
        'format_rules': {
            'Nom_Rue': 'title_case',
            'N': 'numeric',
            'NB_KW_Jour': 'numeric'
        },
        'business_rules': [
            {
                'name': 'validate_consumption_positive',
                'condition': "NB_KW_Jour < 0",
                'action': {
                    'type': 'set_value',
                    'column': 'NB_KW_Jour',
                    'value': 0
                }
            },
            {
                'name': 'flag_high_consumption',
                'condition': "NB_KW_Jour > 50",
                'action': {
                    'type': 'flag',
                    'flag_column': 'High_Consumption_Flag',
                    'flag_value': True
                }
            }
        ]
    }