# Pipeline de Dados de Lançamentos da SpaceX

Este projeto implementa um pipeline de dados completo que extrai informações sobre os lançamentos de foguetes da SpaceX, enriquece esses dados com taxas de câmbio e os disponibiliza em um Data Lake, além de gerar um dashboard interativo para análise.

O projeto atende a todos os requisitos do desafio prático de Python para Engenharia de Dados.

---

## Arquitetura do Pipeline

O pipeline é dividido em três etapas principais, orquestradas por scripts Python:

1.  **Extração e Transformação (ETL):** O script `src/extract_api_spacex.py` é o coração do pipeline. Ele:
    - Extrai dados de duas APIs públicas distintas: **SpaceX API** (dados de lançamentos) e **Frankfurter.app API** (cotações de moeda).
    - Realiza a limpeza, transformação e enriquecimento dos dados, calculando o custo dos lançamentos em BRL.
    - Persiste os dados em duas formas: um **Data Lake** em formato Parquet particionado e um arquivo **Excel** para análise manual.

2.  **Qualidade de Dados:** O script `src/quality_report.py` lê os dados do Data Lake e gera um relatório em JSON com métricas essenciais, como contagem de registros, percentual de nulos e tipos de dados.

3.  **Visualização:** O script `src/dashboard.py` utiliza a biblioteca Streamlit para criar um dashboard interativo que consome os dados do Data Lake e o relatório de qualidade para apresentar análises visuais.

---

## Como Executar o Projeto

### Pré-requisitos

- Python 3.8 ou superior
- `pip` (gerenciador de pacotes do Python)

### 1. Clone o Repositório

```bash
git clone <URL_DO_SEU_REPOSITORIO>
cd DataFlow
```

### 2. Execute o Pipeline de Dados

Para executar todo o processo de ETL e gerar o relatório de qualidade, utilize o script `run.sh`. Ele irá automaticamente instalar as dependências e executar os scripts na ordem correta.

**Importante:** Dê permissão de execução ao script primeiro.

```bash
chmod +x run.sh
./run.sh
```

Ao final da execução, os dados estarão em `data/lake/` e o relatório de qualidade em `output/quality_report.json`.

### 3. Visualize o Dashboard

Após a execução bem-sucedida do pipeline, você pode iniciar o dashboard interativo com o seguinte comando:

```bash
streamlit run src/dashboard.py
```

O Streamlit irá fornecer uma URL local (geralmente `http://localhost:8501`) que você pode abrir no seu navegador para ver o dashboard.

---

## Estrutura do Projeto

```
/DataFlow
├── data/
│   ├── lake/                  # Data Lake com dados em Parquet particionados
│   └── launches_for_analysis.xlsx # Saída em Excel para exploração
├── docs/
│   └── ...                    # Documentação do desafio
├── output/
│   └── quality_report.json    # Relatório de qualidade de dados
├── src/
│   ├── extract_api_spacex.py  # Script principal do ETL
│   ├── quality_report.py      # Script para gerar o relatório de qualidade
│   └── dashboard.py           # Script do dashboard Streamlit
├── .gitignore
├── README.md                  # Este arquivo
├── requirements.txt           # Dependências do projeto
└── run.sh                     # Script para executar o pipeline
```

---

## APIs Utilizadas

1.  **SpaceX API**
    - **Endpoint (Lançamentos):** `https://api.spacexdata.com/v3/launches`
    - **Endpoint (Foguetes):** `https://api.spacexdata.com/v3/rockets`
    - **Descrição:** Fornece dados detalhados sobre todos os lançamentos históricos da SpaceX, incluindo informações sobre os foguetes utilizados.
    - **Autenticação:** Nenhuma.

2.  **Frankfurter.app API**
    - **Endpoint (Taxas Históricas):** `https://api.frankfurter.app/{YYYY-MM-DD}?from=USD&to=BRL`
    - **Descrição:** API gratuita e de código aberto para taxas de câmbio, utilizando dados do Banco Central Europeu.
    - **Autenticação:** Nenhuma.
