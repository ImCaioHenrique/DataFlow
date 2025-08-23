import os
import pandas as pd
import json
from datetime import datetime
import numpy as np

def convert_to_serializable(obj):
    """Converts objects in a dictionary to JSON-serializable types."""
    if isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(i) for i in obj]
    elif isinstance(obj, (np.integer, np.floating, np.bool_)):
        return obj.item()
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    return obj

def generate_quality_report(data_path, output_path):
    """
    Reads a Parquet dataset, calculates quality metrics, and saves a report in JSON.
    """
    print("--- Starting Data Quality Report Generation ---")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    try:
        print(f"Reading data from: {data_path}")
        df = pd.read_parquet(data_path)
    except Exception as e:
        print(f"Error reading dataset at {data_path}. Has the main pipeline been executed?")
        print(e)
        return

    print(f"Total rows: {len(df)}")
    print("Calculating quality metrics...")

    # Separate numeric and non-numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    non_numeric_cols = df.select_dtypes(exclude=[np.number]).columns

    # Descriptive stats for numeric columns
    numeric_stats = df[numeric_cols].describe().to_dict()

    # Descriptive stats for non-numeric columns (e.g., categorical, datetime)
    non_numeric_stats = df[non_numeric_cols].describe().to_dict()

    # Combine stats
    all_stats = {**numeric_stats, **non_numeric_stats}

    # Calculate range checks for numeric columns
    range_checks = {
        col: {
            "min": float(df[col].min()) if col in numeric_cols else None,
            "max": float(df[col].max()) if col in numeric_cols else None
        } for col in df.columns
    }

    quality_report = {
        "gerado_em": datetime.now().isoformat(),
        "fonte_dados": data_path,
        "sumario_qualidade": {
            "total_registros": len(df),
            "percentual_nulos_por_coluna": {col: f"{(df[col].isnull().sum() / len(df)) * 100:.2f}%" for col in df.columns},
            "tipos_de_dados": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "verificacao_de_intervalo": range_checks
        },
        "estatisticas_descritivas": all_stats
    }

    # Convert to JSON-serializable format
    serializable_report = convert_to_serializable(quality_report)

    print(f"Saving quality report to: {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(serializable_report, f, indent=4, ensure_ascii=False)
    
    print("--- Data Quality Report Completed Successfully! ---")

if __name__ == "__main__":
    LAKE_PATH = "data/lake/launches"
    REPORT_OUTPUT_PATH = "output/quality_report.json"
    generate_quality_report(data_path=LAKE_PATH, output_path=REPORT_OUTPUT_PATH)