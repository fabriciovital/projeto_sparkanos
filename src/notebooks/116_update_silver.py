import logging
import os
from datetime import datetime

import pyspark
from configs import configs
from dotenv import load_dotenv
from functions import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

load_dotenv()

HOST_ADDRESS = os.getenv("HOST_ADDRESS")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")


def configure_spark():
    """Configure and return a SparkSession."""
    spark = (
        SparkSession.builder.appName("update_silver")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{HOST_ADDRESS}:9000")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("hive.metastore.uris", "thrift://metastore:9083")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    return spark


def ingest_data():

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("Starting ingestion...")

    input_prefix_layer = configs.prefix_layer_name["1"]
    storage_input = configs.lake_path["bronze"]

    output_prefix_layer = configs.prefix_layer_name["2"]
    storage_output = configs.lake_path["silver"]

    for table_name, query in configs.tables_silver.items():

        input_path = f"{storage_input}{input_prefix_layer}{table_name}"
        output_path = f"{storage_output}{output_prefix_layer}{table_name}"

        try:
            max_modified_date_destination = (
                spark.read.format("delta")
                .load(output_path)
                .select(func.max("modifieddate").alias("max_modifieddate"))
                .collect()[0]["max_modifieddate"]
            )
            # print(max_modified_date_destination)

            df_input_data = (
                spark.read.format("delta")
                .load(input_path)
                .filter(
                    func.col("modifieddate") > func.lit(max_modified_date_destination)
                )
            )

            input_data_count = df_input_data.count()
            logging.info(
                f"Number os rows processed for table {table_name}: {input_data_count}"
            )

            if input_data_count == 0:
                logging.info(f"No new data to process for table {table_name}.")
                continue

            df_with_update_date = F.add_metadata(df_input_data)
            df_with_month_key = F.add_month_key(df_with_update_date, "modifieddate")

            df_with_month_key.write.format("delta").mode("append").partitionBy(
                "month_key"
            ).save(output_path)

        except Exception as e:
            logging.error(f"Error processing table {table_name}: {str(e)}.")

    logging.info("Ingestion completed!")


if __name__ == "__main__":
    spark = configure_spark()
    ingest_data()
