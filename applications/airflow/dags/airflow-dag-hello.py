from airflow.decorators import dag, task
from datetime import datetime
import pendulum

@dag(
    schedule_interval='@once',  # Executar uma vez
    start_date=datetime(2024, 2, 24),
    catchup=False,
    tags=["example"],
    description='Um simples DAG de Hello World com decoradores'
)
def airflow_messages():
    @task(task_id='print_hello')
    def say_hello():
        print("Hello, World!")

    @task(task_id='print_goodbye')
    def say_goodbye():
        print("Goodbye Airflow!")


    say_hello() >> say_goodbye()

airflow_messages()
