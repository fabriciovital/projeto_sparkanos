FROM wlcamargo/spark-master

# Etl Adventure Works

# Use root to set up the environment
USER root

RUN mkdir -p /app

# Setup App Adventure Works
COPY src/notebooks/configs /app/configs/
COPY src/notebooks/functions /app/functions/

COPY src/notebooks/sample_airflow.py /app/

# Spark Configs
COPY applications/spark/conf/env /env/
COPY applications/spark/conf/util /util/

WORKDIR /app

# Switch to a non-root user for running the application (add a new user if needed)
RUN useradd -ms /bin/bash spark
USER spark