from airflow import DAG
from airflow.ops.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Adiciona o caminho do seu projeto para o Airflow encontrar os scripts no /src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Importa as funções principais dos seus arquivos existentes
from bronzedelta import processar_bronze  # Supondo que você encapsulou o código em funções
from silver_vault import processar_silver
from gold_projetos import processar_gold
from visualizar_gold import gerar_grafico

# 1. Definições padrão da DAG
default_args = {
    'owner': 'engenheiro_dados',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Instanciando a DAG
with DAG(
    'pipeline_minc_rouanet',
    default_args=default_args,
    description='Orquestração completa do Lakehouse MinC',
    schedule_interval='@daily',  # Roda todo dia à meia-noite
    catchup=False
) as dag:

    # 3. Definição das Tarefas (Tasks)
    
    task_bronze = PythonOperator(
        task_id='ingestao_bronze',
        python_callable=processar_bronze
    )

    task_silver = PythonOperator(
        task_id='modelagem_silver_vault',
        python_callable=processar_silver
    )

    task_gold = PythonOperator(
        task_id='consolidacao_gold',
        python_callable=processar_gold
    )

    task_viz = PythonOperator(
        task_id='geracao_grafico_viz',
        python_callable=gerar_grafico
    )

    # 4. Definição da Dependência (O fluxo do desenho)
    # Bronze -> Silver -> Gold -> Visualização
    task_bronze >> task_silver >> task_gold >> task_viz