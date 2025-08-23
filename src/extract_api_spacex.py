import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import pyarrow as pa
import pyarrow.parquet as pq
import shutil

# --- Funções de Extração da API SpaceX ---

def fetch_spacex_launches():
    """Buscando dados de lançamentos da API da SpaceX API."""
    url = "https://api.spacexdata.com/v3/launches"
    print(f"Buscando dados de: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return pd.json_normalize(response.json())

def fetch_spacex_rockets():
    """Buscando dados de custo por foguete SpaceX API."""
    url = "https://api.spacexdata.com/v3/rockets"
    print(f"Buscando dados de: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return pd.json_normalize(response.json())

# --- Funções de Cotação (usando a API Frankfurter.app) ---

exchange_rate_cache = {}

def get_exchange_rate_for_date(date):
    """
    Busca a cotação do dólar para uma data específica usando a API Frankfurter.app.
    Mantém um cache e tenta dias anteriores em caso de falha ou feriados.
    """
    if pd.isna(date):
        return None
    
    date_key = date.strftime('%Y-%m-%d')
    if date_key in exchange_rate_cache:
        return exchange_rate_cache[date_key]

    print(f"Buscando cotação para data base: {date_key}")
    current_date = date
    for attempt in range(5):
        formatted_date_for_api = current_date.strftime('%Y-%m-%d')
        api_url = f"https://api.frankfurter.app/{formatted_date_for_api}?from=USD&to=BRL"
        
        try:
            response = requests.get(api_url, timeout=15)
            if response.status_code == 200:
                data = response.json()
                rate = data.get('rates', {}).get('BRL')
                if rate:
                    print(f"  Sucesso! Cotação encontrada para {formatted_date_for_api}: {rate}")
                    exchange_rate_cache[date_key] = rate
                    return rate
            else:
                print(f"  Falha na API para {formatted_date_for_api}: Status {response.status_code}. (Tentativa {attempt+1}/5)")

        except requests.exceptions.RequestException:
            print(f"  Erro de conexão para {formatted_date_for_api}. (Tentativa {attempt+1}/5)")
            pass
        
        time.sleep(0.1)
        current_date -= timedelta(days=1)

    print(f"x Falha total em encontrar cotação para data base: {date_key}")
    exchange_rate_cache[date_key] = None
    return None

# --- Orquestração do Script ---

def main():
    """Função principal para orquestrar o processo de ETL."""
    
    # 1. EXTRAÇÃO
    print("--- Iniciando Processo de Extração ---")
    launches_df = fetch_spacex_launches()
    rockets_df = fetch_spacex_rockets()
    
    # 2. TRANSFORMAÇÃO
    print("\n--- Iniciando Processo de Transformação ---")
    launches_df['launch_date_utc'] = pd.to_datetime(launches_df['launch_date_utc'], utc=True)
    launches_df['year'] = launches_df['launch_date_utc'].dt.year
    launches_df['year_month'] = launches_df['launch_date_utc'].dt.strftime('%Y-%m')

    launches_df = launches_df[['flight_number', 'launch_date_utc', 'rocket.rocket_id', 'launch_success', 'year', 'year_month']]
    rockets_df = rockets_df[['rocket_id', 'rocket_name', 'cost_per_launch']]

    print("Juntando dados de lançamentos e foguetes...")
    merged_df = pd.merge(
        launches_df,
        rockets_df,
        left_on='rocket.rocket_id',
        right_on='rocket_id',
        how='left'
    )

    print("Buscando cotações do dólar (USD->BRL) para cada lançamento (isso pode levar alguns minutos)...")
    merged_df['exchange_rate'] = merged_df['launch_date_utc'].dt.tz_localize(None).apply(get_exchange_rate_for_date)
    
    print("Calculando custo em BRL e limpando dados...")
    merged_df['cost_in_brl'] = merged_df['cost_per_launch'] * merged_df['exchange_rate']
    merged_df['cost_in_brl'] = merged_df['cost_in_brl'].round(2)
    final_df = merged_df.copy()

    # 3. CARREGAMENTO (LOAD)
    print("\n--- Iniciando Processo de Carregamento ---")
    
    # Saída 1: Data Lake em Parquet Particionado (Idempotente)
    lake_path = "data/lake/launches"
    
    # Remove o diretório antigo para garantir a idempotência
    if os.path.exists(lake_path):
        print(f"Removendo diretório do lake existente em: {lake_path}")
        shutil.rmtree(lake_path)
    
    print(f"Salvando dados no Data Lake em: {lake_path}")
    final_df['launch_date_utc'] = final_df['launch_date_utc'].dt.tz_localize(None)
    table = pa.Table.from_pandas(final_df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=lake_path,
        partition_cols=['year']
    )

    # Saída 2: Arquivo Excel para Análise Rápida
    excel_path = "data/launches_for_analysis.xlsx"
    print(f"Salvando arquivo Excel para análise em: {excel_path}")
    try:
        final_df.to_excel(excel_path, index=False, engine='openpyxl')
    except ImportError:
        print("\nAVISO: A biblioteca 'openpyxl' não está instalada. A exportação para Excel foi ignorada.")
        print("Para habilitar, execute: pip install openpyxl")

    print("\n--- Processo Concluído com Sucesso! ---")

if __name__ == "__main__":
    main()
