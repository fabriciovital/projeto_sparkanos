# Overview
This script is designed to convert data stored in Parquet format in a MinIO-based landing zone to Delta format in the bronze layer, also stored in MinIO. It uses Apache Spark for distributed processing and partitions the data by a month key before saving. The process includes adding metadata to the data before conversion.

## Libraries

- **`pyspark`**: The core library for Apache Spark, providing the SparkSession and DataFrame functionalities.
- **`logging`**: Standard Python library for logging messages during the script execution.
- **`datetime`**: Standard Python library for working with dates and times (though not directly used in this script).
- **`pyspark.sql.functions`**: Provides built-in functions for DataFrame operations.
- **`dotenv`**: Library to load environment variables from a `.env` file.
- **`os`**: Standard Python library for interacting with the operating system and environment variables.
- **`configs`**: Custom module containing configuration settings used by the script.
- **`functions`**: Custom module (aliased as `F`) containing external functions used for data processing.

## Variables

### Environment Variables
These variables are loaded from a `.env` file using `dotenv` and are used to configure the Spark session and access credentials:

- **`HOST_ADDRESS`**: The address of the host machine where MinIO is running.
- **`MINIO_ACCESS_KEY`**: Access key for MinIO.
- **`MINIO_SECRET_KEY`**: Secret key for MinIO.

## Spark Session Configuration
- **`spark`**: Initializes a Spark session with various configurations for connecting to MinIO (S3A) and Hive Metastore, and enables Delta Lake support.

## External Functions
The script imports external functions from the `configs` and `functions` modules:

### From `configs`:
- **`configs.lake_path['landing_adventure_works']`**: The base path for reading Parquet files from the landing zone.
- **`configs.prefix_layer_name['1']`**: The prefix used for the output path in the bronze layer.
- **`configs.lake_path['bronze']`**: The base path for storing processed Delta tables in the bronze layer.

### From `functions` (aliased as `F`):
- **`F.convert_table_name(table_input_name)`**: Converts a logical table name to a format suitable for use in the data lake.
- **`F.add_metadata(df_input_data)`**: Adds metadata, such as processing timestamps, to the DataFrame.

## Main Logic

### 1. Initialization:
- **Spark Session**: A Spark session is created with configurations to interface with MinIO using the S3A protocol, and Hive Metastore for metadata management. Delta Lake is enabled through Spark extensions.
- **Logging**: Logging is configured to record informational messages and errors throughout the process.

### 2. Processing Tables:
- The script iterates over the tables defined in `configs.tables_postgres_adventureworks`.

For each table:
- **Table Name Conversion**: The table name is converted to a format suitable for the data lake using `F.convert_table_name`.
- **Data Extraction**: Data is read from Parquet files in the landing zone using Spark.
- **Add Metadata**: Metadata such as update timestamps are added to the DataFrame using `F.add_metadata`.
- **Save Data**: The processed DataFrame is saved in Delta format to the bronze layer, partitioned by the `month_key`.
- **Error Handling**: If an error occurs during processing, it is logged.

### 3. Completion:
- After all tables are processed, a log message is generated indicating that the conversion from Parquet to Delta was successful.
