import streamlit as st
import pandas as pd
import plotly.express as px
import json

# --- Configuração da Página ---
st.set_page_config(
    page_title="Dashboard de Lançamentos SpaceX",
    page_icon="🚀",
    layout="wide"
)

# --- Funções de Carregamento de Dados ---
@st.cache_data
def load_data(path):
    """Carrega o dataset de lançamentos do Data Lake."""
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        st.error(f"Arquivo de dados não encontrado em: {path}. Execute o pipeline principal primeiro.")
        return None

@st.cache_data
def load_quality_report(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data
    except FileNotFoundError:
        st.error(f"Relatório de qualidade não encontrado em: {path}. Execute o script de qualidade primeiro.")
        return None
    except json.JSONDecodeError:
        st.error(f"Erro ao decodificar o JSON em: {path}. Verifique se o arquivo está no formato correto.")
        return None

# --- Carregamento dos Dados ---
LAKE_PATH = "data/lake/launches"
REPORT_PATH = "output/quality_report.json"

df = load_data(LAKE_PATH)
quality_report = load_quality_report(REPORT_PATH)

# --- Título do Dashboard ---
st.title("🚀 Dashboard de Análise de Lançamentos da SpaceX")
st.markdown("Este dashboard apresenta análises sobre os lançamentos de foguetes da SpaceX.")

# --- Visualizações ---
if df is not None:
    # Prepara o dataframe para as visualizações
    df['year'] = pd.to_datetime(df['launch_date_utc']).dt.year
    df['launch_success'] = df['launch_success'].fillna(False) # Assume que nulo é falha para a taxa de sucesso

    # 1. Taxa de Sucesso por Ano
    st.header("📈 Taxa de Sucesso de Lançamentos por Ano")
    success_rate_by_year = df.groupby('year')['launch_success'].mean().reset_index()
    success_rate_by_year['launch_success'] = success_rate_by_year['launch_success'] * 100
    
    fig_success = px.line(
        success_rate_by_year,
        x='year',
        y='launch_success',
        title='Percentual de Sucesso dos Lançamentos ao Longo dos Anos',
        labels={'year': 'Ano', 'launch_success': 'Taxa de Sucesso (%)'},
        markers=True
    )
    fig_success.update_yaxes(range=[0, 110])
    st.plotly_chart(fig_success, use_container_width=True)

    # 2. Comparação de Custo por Ano (USD vs BRL)
    st.header("💰 Custo Total Anual dos Lançamentos (USD vs BRL)")
    cost_by_year = df.groupby('year')[['cost_per_launch', 'cost_in_brl']].sum().reset_index()
    
    # Melt para facilitar a plotagem com Plotly
    cost_melted = cost_by_year.melt(
        id_vars=['year'], 
        value_vars=['cost_per_launch', 'cost_in_brl'], 
        var_name='moeda', 
        value_name='custo_total'
    )
    cost_melted['moeda'] = cost_melted['moeda'].map({'cost_per_launch': 'USD', 'cost_in_brl': 'BRL'})

    fig_cost = px.bar(
        cost_melted,
        x='year',
        y='custo_total',
        color='moeda',
        barmode='group',
        title='Comparação de Custo Total Anual em USD e BRL',
        labels={'year': 'Ano', 'custo_total': 'Custo Total (em bilhões)', 'moeda': 'Moeda'}
    )
    st.plotly_chart(fig_cost, use_container_width=True)

# --- Tabela de Qualidade de Dados ---
st.header("📊 Relatório de Qualidade dos Dados")
if quality_report:
    try:
        st.markdown("Visão geral da qualidade do dataset processado.")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total de Registros", quality_report['quality_summary']['total_records'])
        with col2:
            st.metric("Gerado em", pd.to_datetime(quality_report['generated_at']).strftime('%d/%m/%Y %H:%M:%S'))

        st.subheader("Percentual de Nulos por Coluna")
        null_ratios = quality_report['quality_summary']['null_percentage_per_column']
        st.dataframe(pd.DataFrame.from_dict(null_ratios, orient='index', columns=['Percentual de Nulos']), use_container_width=True)
    except KeyError as e:
        st.error(f"Erro no formato do relatório de qualidade: chave {e} não encontrada.")
        st.write("Debug: Estrutura do relatório carregado:", quality_report)
else:
    st.warning("Não foi possível carregar o relatório de qualidade. Execute o script de qualidade primeiro.")
