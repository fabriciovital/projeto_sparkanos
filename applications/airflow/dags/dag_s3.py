from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator

default_args = {
  'owner': 'Wallace Camargo',
  'depends_on_post': False,
  'email': ['wallacecpdg@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
}

with DAG(
    'dag_s3_sensor',
    default_args=default_args,
    start_date=datetime.now(),
    schedule_interval='@weekly',
    catchup=False,  # Adiciona este parâmetro para evitar a execução de tarefas passadas
    tags=['test', 'development', 'bash']
  ) as dag:

  sensor = S3KeySensor(
    task_id='sensor',
    aws_conn_id='conn_minio',
    bucket_name='landing-zone',
    bucket_key='titles.csv',
  )

  is_ok = BashOperator(
    task_id= "is_ok",
    bash_command = "echo 'arquivo está na pasta'"
  )  

sensor >> is_ok