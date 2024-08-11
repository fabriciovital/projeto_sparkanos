host = '172.21.121.140'

services = {
    'minio': (f'http://{host}:9001', 'MinIO Console'),
    'superset': (f'http://{host}:8088', 'Superset'),
    'trino': (f'http://{host}:8080/ui/login.html','Cluster Overview - Trino'),
    'jupyter': (f'http://{host}:8889/lab?', 'JupyterLab'),
    'Airflow': (f'http://{host}:8081/login/', 'Sign In - Airflow')   
}
