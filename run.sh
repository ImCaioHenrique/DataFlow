#!/bin/bash

# Garante que o script pare se algum comando falhar
set -e

# --- Instalação de Dependências ---
echo "Verificando e instalando dependências do requirements.txt..."
pip install -r requirements.txt

# --- Execução do Pipeline de Dados ---
echo "\n>>> PASSO 1: Executando o pipeline principal de ETL (Extração, Transformação, Carga)..."
python3 src/extract_api_spacex.py

# --- Geração do Relatório de Qualidade ---
echo "\n>>> PASSO 2: Gerando o relatório de qualidade dos dados..."
python3 src/quality_report.py

# --- Mensagem de Conclusão ---
echo "\n--- Pipeline de Dados Concluído com Sucesso! ---"
echo "Os dados foram processados e estão no diretório 'data/lake'."
echo "O relatório de qualidade foi salvo em 'output/quality_report.json'."

# --- Instruções para o Dashboard ---
echo "\nPara visualizar os dados, execute o dashboard com o seguinte comando:"
echo "streamlit run src/dashboard.py"
