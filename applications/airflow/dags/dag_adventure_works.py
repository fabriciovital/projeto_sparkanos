from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'Wallace Camargo',
    'depends_on_past': False,
}

# Definição da função run_container
def run_container(dag, image, container_name, command):
    runner = DockerOperator(
        task_id=container_name,
        image=image,
        container_name=container_name,
        api_version='auto',
        auto_remove=True,
        command=command,
        docker_url="tcp://docker-proxy:2375",
        network_mode="spark_arruda_bigdata",
        mount_tmp_dir=False,  # Disable mounting the temporary directory
        dag=dag  # Passando a referência da DAG para o operador
    )
    return runner

# Definição da DAG
with DAG(
    'adventure_works',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),  # Use a fixed start date
    schedule_interval='@weekly',
    catchup=False,
    tags=['postgres', 'delta lake', 'comercial']
) as dag:

    with TaskGroup(group_id="etl-adventure-works") as ingestion_bronze_group:
        ingestion_parquet = run_container(
            dag=dag,
            image='wlcamargo/spark-etl-adventure-works',
            container_name='ingestion_parquet',
            command="spark-submit /app/112_update_landing.py"
        )

        ingestion_bronze = run_container(
            dag=dag,
            image='wlcamargo/spark-etl-adventure-works',
            container_name='ingestion_bronze',
            command="spark-submit /app/113_update_bronze.py"
        )
        
        processing_silver = run_container(
            dag=dag,
            image='wlcamargo/spark-etl-adventure-works',
            container_name='processing_silver',
            command="spark-submit /app/114_update_silver.py"
        )

        refinement_gold = run_container(
            dag=dag,
            image='wlcamargo/spark-etl-adventure-works',
            container_name='refinement_gold',
            command="spark-submit /app/115_update_gold.py"
        )

        # Definindo as dependências entre as tarefas dentro do grupo
        ingestion_parquet >> ingestion_bronze >> processing_silver >> refinement_gold

# Adicionando o grupo de tarefas à DAG
ingestion_bronze_group
