import pandas as pd
import numpy as np
import json
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

def check_completeness(df, columns=None):
    """Check for missing values in specified columns or all columns."""
    if columns is None:
        columns = df.columns
    
    total_rows = len(df)
    missing_counts = df[columns].isnull().sum()
    missing_percentages = (missing_counts / total_rows * 100).round(2)
    
    completeness_report = {}
    for col in columns:
        completeness_report[col] = {
            'missing_count': int(missing_counts[col]),
            'missing_percentage': float(missing_percentages[col]),
            'complete_count': int(total_rows - missing_counts[col]),
            'completeness_score': float(100 - missing_percentages[col])
        }
    
    return completeness_report

def check_duplicates(df, subset=None):
    """Check for duplicate rows with detailed analysis."""
    total_rows = len(df)
    duplicate_rows = df.duplicated(subset=subset, keep=False)
    unique_duplicates = df.duplicated(subset=subset, keep='first')
    
    return {
        'total_rows': total_rows,
        'duplicate_count': int(unique_duplicates.sum()),
        'duplicate_percentage': float((unique_duplicates.sum() / total_rows * 100).round(2)),
        'unique_rows': int(total_rows - unique_duplicates.sum()),
        'uniqueness_score': float(((total_rows - unique_duplicates.sum()) / total_rows * 100).round(2))
    }

def check_format_consistency(df, column, expected_patterns=None):
    """Check format consistency for a column."""
    if column not in df.columns:
        return {'error': f'Column {column} not found'}
    
    series = df[column].dropna()
    if len(series) == 0:
        return {'error': 'No data to check'}
    
    # Basic format analysis
    data_types = series.apply(type).value_counts()
    
    # Check for numeric consistency
    numeric_consistency = pd.to_numeric(series, errors='coerce').notna().sum()
    
    # Check for string patterns if expected
    format_report = {
        'total_non_null': len(series),
        'data_types': {str(k): int(v) for k, v in data_types.items()},
        'numeric_convertible': int(numeric_consistency),
        'format_consistency_score': float((numeric_consistency / len(series) * 100).round(2))
    }
    
    return format_report

def check_codification_consistency(df, column, valid_codes=None):
    """Check codification consistency (e.g., CSP codes)."""
    if column not in df.columns:
        return {'error': f'Column {column} not found'}
    
    series = df[column].dropna()
    if len(series) == 0:
        return {'error': 'No data to check'}
    
    unique_values = series.unique()
    
    if valid_codes:
        valid_count = series.isin(valid_codes).sum()
        invalid_values = [v for v in unique_values if v not in valid_codes]
    else:
        # Try to infer numeric codes
        try:
            numeric_values = pd.to_numeric(series, errors='coerce')
            valid_count = numeric_values.notna().sum()
            invalid_values = series[numeric_values.isna()].unique()
        except:
            valid_count = len(series)
            invalid_values = []
    
    return {
        'total_values': len(series),
        'unique_values': len(unique_values),
        'valid_codes_count': int(valid_count),
        'invalid_codes_count': int(len(series) - valid_count),
        'invalid_values': list(invalid_values),
        'codification_score': float((valid_count / len(series) * 100).round(2))
    }

def check_granularity_consistency(df, group_columns, measure_column):
    """Check granularity and scale consistency."""
    if not all(col in df.columns for col in group_columns + [measure_column]):
        return {'error': 'Required columns not found'}
    
    # Group by specified columns and analyze distribution
    grouped = df.groupby(group_columns)[measure_column].agg(['count', 'mean', 'std', 'min', 'max'])
    
    return {
        'groups_count': len(grouped),
        'avg_records_per_group': float(grouped['count'].mean().round(2)),
        'std_records_per_group': float(grouped['count'].std().round(2)),
        'min_records_per_group': int(grouped['count'].min()),
        'max_records_per_group': int(grouped['count'].max()),
        'measure_statistics': {
            'mean': float(grouped['mean'].mean().round(2)),
            'std': float(grouped['std'].mean().round(2)),
            'min': float(grouped['min'].min().round(2)),
            'max': float(grouped['max'].max().round(2))
        }
    }

def calculate_data_quality_score(completeness_report, duplicates_report, format_reports, codification_reports):
    """Calculate overall data quality score."""
    
    # Completeness score (average of all columns)
    completeness_scores = [report['completeness_score'] for report in completeness_report.values()]
    avg_completeness = np.mean(completeness_scores) if completeness_scores else 0
    
    # Uniqueness score
    uniqueness_score = duplicates_report.get('uniqueness_score', 0)
    
    # Format consistency score (average)
    format_scores = [report.get('format_consistency_score', 0) for report in format_reports.values()]
    avg_format_score = np.mean(format_scores) if format_scores else 0
    
    # Codification score (average)
    codification_scores = [report.get('codification_score', 0) for report in codification_reports.values()]
    avg_codification_score = np.mean(codification_scores) if codification_scores else 0
    
    # Weighted overall score
    overall_score = (
        avg_completeness * 0.3 +
        uniqueness_score * 0.25 +
        avg_format_score * 0.25 +
        avg_codification_score * 0.20
    )
    
    return {
        'completeness_score': round(avg_completeness, 2),
        'uniqueness_score': round(uniqueness_score, 2),
        'format_score': round(avg_format_score, 2),
        'codification_score': round(avg_codification_score, 2),
        'overall_quality_score': round(overall_score, 2),
        'quality_level': get_quality_level(overall_score)
    }

def get_quality_level(score):
    """Get quality level based on score."""
    if score >= 90:
        return 'Excellent'
    elif score >= 80:
        return 'Good'
    elif score >= 70:
        return 'Fair'
    elif score >= 60:
        return 'Poor'
    else:
        return 'Very Poor'

def generate_quality_report(df, source_name, output_path=None):
    """Generate comprehensive quality report for a dataset."""
    
    report = {
        'source_name': source_name,
        'timestamp': datetime.now().isoformat(),
        'dataset_info': {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns)
        }
    }
    
    # Completeness analysis
    report['completeness'] = check_completeness(df)
    
    # Duplicates analysis
    report['duplicates'] = check_duplicates(df)
    
    # Format consistency for numeric columns
    format_reports = {}
    for col in df.columns:
        if df[col].dtype in ['int64', 'float64'] or col in ['CSP', 'ID_CSP', 'NB_KW_Jour']:
            format_reports[col] = check_format_consistency(df, col)
    report['format_consistency'] = format_reports
    
    # Codification consistency for categorical columns
    codification_reports = {}
    if 'CSP' in df.columns:
        codification_reports['CSP'] = check_codification_consistency(df, 'CSP', ['1', '2', '3', '4'])
    report['codification_consistency'] = codification_reports
    
    # Overall quality score
    report['quality_score'] = calculate_data_quality_score(
        report['completeness'],
        report['duplicates'],
        format_reports,
        codification_reports
    )
    
    # Save report if path provided
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
    
    return report

def create_quality_visualizations(report, output_dir):
    """Create visualizations for quality metrics."""
    
    # Set style
    plt.style.use('seaborn-v0_8')
    
    # 1. Completeness visualization
    completeness_data = report['completeness']
    columns = list(completeness_data.keys())
    completeness_scores = [completeness_data[col]['completeness_score'] for col in columns]
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
    
    # Completeness bar chart
    ax1.bar(columns, completeness_scores, color='skyblue', alpha=0.7)
    ax1.set_title('Complétude par Colonne (%)')
    ax1.set_ylabel('Score de Complétude')
    ax1.set_ylim(0, 100)
    ax1.tick_params(axis='x', rotation=45)
    
    # Overall quality score pie chart
    quality_metrics = report['quality_score']
    metrics = ['Complétude', 'Unicité', 'Format', 'Codification']
    scores = [
        quality_metrics['completeness_score'],
        quality_metrics['uniqueness_score'],
        quality_metrics['format_score'],
        quality_metrics['codification_score']
    ]
    
    ax2.pie(scores, labels=metrics, autopct='%1.1f%%', startangle=90)
    ax2.set_title('Répartition des Scores de Qualité')
    
    # Missing values heatmap data
    missing_counts = [completeness_data[col]['missing_count'] for col in columns]
    ax3.bar(columns, missing_counts, color='coral', alpha=0.7)
    ax3.set_title('Valeurs Manquantes par Colonne')
    ax3.set_ylabel('Nombre de Valeurs Manquantes')
    ax3.tick_params(axis='x', rotation=45)
    
    # Overall score gauge
    overall_score = quality_metrics['overall_quality_score']
    quality_level = quality_metrics['quality_level']
    
    # Create a simple gauge representation
    theta = np.linspace(0, np.pi, 100)
    r = np.ones_like(theta)
    
    ax4.plot(theta, r, 'k-', linewidth=2)
    ax4.fill_between(theta, 0, r, alpha=0.3, color='lightgray')
    
    # Color code based on score
    if overall_score >= 80:
        color = 'green'
    elif overall_score >= 60:
        color = 'orange'
    else:
        color = 'red'
    
    score_theta = np.pi * (1 - overall_score/100)
    ax4.plot([score_theta, score_theta], [0, 1], color=color, linewidth=4)
    ax4.text(np.pi/2, 0.5, f'{overall_score}%\n{quality_level}', 
             ha='center', va='center', fontsize=12, fontweight='bold')
    ax4.set_title('Score Global de Qualité')
    ax4.set_xlim(0, np.pi)
    ax4.set_ylim(0, 1.2)
    ax4.axis('off')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/quality_dashboard_{report["source_name"]}.png', dpi=300, bbox_inches='tight')
    plt.close()

def report_quality(df, checks):
    """Run a set of quality checks and return a report as dict."""
    report = {}
    for name, func, args in checks:
        report[name] = func(df, *args)
    return report
