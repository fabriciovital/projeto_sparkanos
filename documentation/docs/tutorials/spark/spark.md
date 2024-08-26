# Spark

## How to Use Spark?
Spark can be used through Jupyter for application development, and orchestration should be handled by Airflow through a job executed within a container.

## Spark Submit
```
spark-submit app_pyspark.py
```

## Cluster mode
```
spark-submit --master spark://spark-master:7077 app_pyspark.py
```

## Config Spark (Cluster Mode)
```
def configure_spark():
    """Configure and return a SparkSession."""
    spark = SparkSession.builder \
        .appName("update_landing") \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{HOST_ADDRESS}:9000") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("hive.metastore.uris", "thrift://metastore:9083") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark
```

## How to Create a Spark Job?

### Creating an Image with a Spark Job:

Navigate to the directory `data/jobs_spark`. Inside it, create the `.py` file with your Spark job. To build the image, use the `wlcamargo/spark-master` image as a base. It contains all the necessary Jars to work with Delta and Iceberg. Example Dockerfile:

```
Dockerfile
```

```
FROM wlcamargo/spark-master

# Etl Adventure Works

# Use root to set up the environment
USER root

RUN pip install --no-cache-dir python-dotenv

RUN mkdir -p /app

# Setup App Adventure Works
COPY src/notebooks/configs /app/configs/
COPY src/notebooks/functions /app/functions/

COPY src/notebooks/114_update_landing.py /app/
COPY src/notebooks/115_update_bronze.py /app/
COPY src/notebooks/116_update_silver.py /app/
COPY src/notebooks/117_update_gold.py /app/

# Spark Configs
COPY applications/spark/conf/env /env/
COPY applications/spark/conf/util /util/

WORKDIR /app

# Switch to a non-root user for running the application (add a new user if needed)
RUN useradd -ms /bin/bash spark
USER spark
```

--------------------------------------------------

### Build the image:
``` 
sudo docker build -t wlcamargo/spark-etl-adventure-works .
```
### Push the image:
``` 
sudo docker push wlcamargo/spark-etl-adventure-works 
```

## How to create new connection?
You need to find the specific JAR file and add it to the folder.

### Path to the directory with JARs
```
JAR_DIR="/home/jovyan/work/jars"
```

### Concatenate all JARs into a comma-separated string
```
JARS=$(echo $JAR_DIR/*.jar | tr ' ' ',')
```

### Use the resulting string with spark-submit
```
spark-submit --jars $JARS your-app.py
```

## Start with Scala
```
pyspark-shell
```

## Start with Pyspark
```
pyspark
```

