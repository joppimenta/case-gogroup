from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_magalu_bigquery',
    default_args=default_args,
    description='Coleta smartphones e processa no dbt (Fluxo Completo)',
    schedule_interval='0 18 * * *', 
    catchup=False,
    tags=['scraping', 'magalu', 'elt'],
) as dag:

    # 1. executar o scrapping do python
    task_extrair_dados = BashOperator(
        task_id='extrair_dados_magalu',
        bash_command='python3 /opt/airflow/collector/extract.py',
    )

    # 2. transformação via dbt
    task_dbt_run = BashOperator(
        task_id='dbt_transform',
        bash_command='''
            cd /opt/airflow/dbt &&
            dbt run \
                --profiles-dir . \
                --log-path /tmp/dbt_logs \
                --target-path /tmp/dbt_target
        ''',
    )

    task_extrair_dados >> task_dbt_run