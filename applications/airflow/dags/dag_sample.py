from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'Wallace Camargo',
    'depends_on_past': False,
}

# Definição da função run_container
def run_container(dag, image, container_name, command):
    return DockerOperator(
        task_id=container_name,
        image=image,
        container_name=container_name,
        api_version='auto',
        auto_remove=True,
        command=command,
        docker_url="tcp://docker-proxy:2375",
        network_mode="sparkanos",
        mount_tmp_dir=False,  # Disable mounting the temporary directory
        dag=dag  # Passando a referência da DAG para o operador
    )

# Definição da DAG
with DAG(
    'sample_airflow',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),  # Use a fixed start date
    schedule_interval='@weekly',
    catchup=False,  # Adiciona este parâmetro para evitar a execução de tarefas passadas
    tags=['sparkanos']
) as dag:

    sample_employee_task = run_container(
        dag=dag,
        image='sparkanos',
        container_name='sample_airflow',
        command="spark-submit /app/111_sample_airflow.py"
    )

    sample_employee_task