# Tree

## Command to draw the tree
```
tree -I 'venv|documentation|__pycache__|*.pyc|*.ipynb_checkpoints'
```

## Project structure 
```
├── LICENSE
├── README.md
├── applications
│   ├── airflow
│   │   ├── dags
│   │   │   ├── __init__.py
│   │   │   ├── airflow-dag-hello.py
│   │   │   ├── dag_adventure_works.py
│   │   │   ├── dag_s3.py
│   │   │   ├── hello.py
│   │   │   ├── sample_employee.py
│   │   │   └── taskflow_api.py
│   │   ├── docker-compose.yml
│   │   └── secret-airflow.txt
│   ├── minio
│   │   └── docker-compose.yml
│   ├── postgres_adventureworks
│   │   └── docker-compose.yml
│   ├── spark
│   │   ├── conf
│   │   │   ├── env
│   │   │   │   ├── jupyter.env
│   │   │   │   ├── start-master.sh
│   │   │   │   └── start-worker.sh
│   │   │   └── util
│   │   ├── docker-compose.yml
│   │   └── util
│   │       ├── core-site.xml
│   │       ├── hadoop-aws-3.3.1.jar
│   │       ├── hdfs-site.xml
│   │       ├── hive-site.xml
│   │       ├── postgresql-42.5.1.jar
│   │       └── trino-jdbc-406.jar
│   ├── superset
│   │   ├── docker
│   │   │   ├── README.md
│   │   │   ├── docker-bootstrap.sh
│   │   │   ├── docker-ci.sh
│   │   │   ├── docker-frontend.sh
│   │   │   ├── docker-init.sh
│   │   │   ├── frontend-mem-nag.sh
│   │   │   ├── pythonpath_dev
│   │   │   │   ├── superset_config.py
│   │   │   │   └── superset_config_local.example
│   │   │   ├── requirements-local.txt
│   │   │   └── run-server.sh
│   │   ├── docker-compose.yml
│   │   └── key-superset.txt
│   └── trino
│       ├── conf
│       │   ├── delta.properties
│       │   ├── hive.properties
│       │   ├── iceberg.properties
│       │   ├── postgres.properties
│       │   └── sqlserver.properties
│       ├── docker-compose.yml
│       └── metastore
│           ├── core-site.xml
│           └── metastore-site.xml
├── requirements.txt
└── src
    └── notebooks
```
