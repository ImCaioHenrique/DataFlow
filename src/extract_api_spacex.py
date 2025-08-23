import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

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
    for attempt in range(5):  # Tenta a data e os 4 dias anteriores
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
                    print(f"  API não retornou cotação para {formatted_date_for_api}. (Fim de semana/feriado? Tentativa {attempt+1}/5)")
            else:
                 print(f"  Falha na API para {formatted_date_for_api}: Status {response.status_code}. (Tentativa {attempt+1}/5)")

        except requests.exceptions.RequestException as e:
            print(f"  Erro de conexão para {formatted_date_for_api}: {e}. (Tentativa {attempt+1}/5)")
        
        time.sleep(0.2)
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
    launches_df['year_month'] = launches_df['launch_date_utc'].dt.strftime('%Y-%m')

    launches_df = launches_df[['flight_number', 'launch_date_utc', 'rocket.rocket_id', 'launch_success', 'year_month']]
    rockets_df = rockets_df[['rocket_id', 'rocket_name', 'cost_per_launch']]

    print("Juntando dados de lançamentos e foguetes...")
    merged_df = pd.merge(
        launches_df,
        rockets_df,
        left_on='rocket.rocket_id',
        right_on='rocket_id',
        how='left'
    )

    print("\nBuscando cotações do dólar (USD->BRL) para cada lançamento (isso pode levar alguns minutos)...")
    merged_df['exchange_rate'] = merged_df['launch_date_utc'].dt.tz_localize(None).apply(get_exchange_rate_for_date)
    
    print("\nCalculando custo em BRL...")
    merged_df['cost_in_brl'] = merged_df['cost_per_launch'] * merged_df['exchange_rate']
    merged_df['cost_in_brl'] = merged_df['cost_in_brl'].round(2)

    # 3. CARREGAMENTO (LOAD)
    print("\n--- Iniciando Processo de Carregamento ---")
    output_path = "data/launches.xlsx"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Remove timezone para salvar em Excel
    merged_df['launch_date_utc'] = merged_df['launch_date_utc'].dt.tz_localize(None)

    print(f"Salvando dados em arquivo Excel: {output_path}")
    try:
        merged_df.to_excel(output_path, index=False, engine='openpyxl')
        print("\n--- Processo Concluído com Sucesso! ---")
        print(f"Arquivo salvo em: {os.path.abspath(output_path)}")
    except ImportError:
        print("erro" )
        

if __name__ == "__main__":
    main()
