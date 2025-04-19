from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='dag_ifood_ingesta_img',
    default_args=default_args,
    description='Executa o pipeline de ingestão e DQ no Airflow',
    schedule_interval=None,
    start_date=datetime(2025, 4, 18),
    catchup=False,
) as dag:

    # Primeira tarefa: Executar o component-scripts/src/main.py
    run_component_scripts = DockerOperator(
        task_id='run_component_scripts',
        image='book_tridata_yellow:latest',
        api_version='auto',
        auto_remove=True,
        command='python component-scripts/src/main.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        working_dir='/app',
    )

    # Segunda tarefa: Executar o component-DQ/src/main.py
    run_component_dq = DockerOperator(
        task_id='run_component_dq',
        image='book_tridata_yellow:latest',
        api_version='auto',
        auto_remove=True,
        command='python component-DQ/src/main.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        working_dir='/app',
    )

    # Definir a ordem de execução
    run_component_scripts >> run_component_dq