import pandas as pd

def check_completeness(df, columns=None):
    """Check for missing values in specified columns or all columns."""
    if columns is None:
        columns = df.columns
    missing = df[columns].isnull().sum()
    return missing

def check_duplicates(df, subset=None):
    """Check for duplicate rows."""
    return df.duplicated(subset=subset).sum()

def check_format(df, column, dtype):
    """Check if a column matches the expected dtype."""
    return df[column].apply(lambda x: isinstance(x, dtype)).all()

def report_quality(df, checks):
    """Run a set of quality checks and return a report as dict."""
    report = {}
    for name, func, args in checks:
        report[name] = func(df, *args)
    return report
