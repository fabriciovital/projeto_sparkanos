FROM wlcamargo/spark-master

# Etl Adventure Works

# Use root to set up the environment
USER root

RUN mkdir -p /app

RUN pip install --no-cache-dir python-dotenv

# Setup App Adventure Works
COPY src/notebooks/configs /app/configs/
COPY src/notebooks/functions /app/functions/

# Notebooks
COPY src/notebooks/.env /app/
COPY src/notebooks/114_update_landing.py /app/
COPY src/notebooks/115_update_bronze.py /app/
COPY src/notebooks/116_update_silver.py /app/
COPY src/notebooks/117_update_gold.py /app/
COPY src/notebooks/examples/111_sample_airflow.py /app/

# Spark Configs
COPY applications/spark/conf/env /env/
COPY applications/spark/conf/util /util/

WORKDIR /app

# Switch to a non-root user for running the application (add a new user if needed)
RUN useradd -ms /bin/bash spark
USER spark