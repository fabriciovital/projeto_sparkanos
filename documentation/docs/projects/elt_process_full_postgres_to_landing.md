# Overview
This script is designed to ingest data from a PostgreSQL database (AdventureWorks) into a landing zone in MinIO, using Apache Spark for distributed processing. The data is saved in Parquet format, partitioned by month, and enriched with metadata. The script logs its progress and any errors encountered during processing.

## Libraries

- **`pyspark`**: Core library for Apache Spark, providing functionalities such as `SparkSession` and `DataFrame` operations.
- **`logging`**: Standard Python library for logging messages during script execution.
- **`configs`**: Custom module containing configuration settings used by the script.
- **`functions`**: Custom module (aliased as `F`) containing external functions for data processing.
- **`dotenv`**: Library to load environment variables from a `.env` file.
- **`os`**: Standard Python library for interacting with the operating system and environment variables.

## Variables

### Environment Variables
These variables are loaded from a `.env` file using `dotenv` and are used to configure the Spark session and access credentials:

- **`HOST_ADDRESS`**: The address of the host machine where PostgreSQL and MinIO are running.
- **`MINIO_ACCESS_KEY`**: Access key for MinIO.
- **`MINIO_SECRET_KEY`**: Secret key for MinIO.
- **`USER_POSTGRES`**: Username for the PostgreSQL database.
- **`PASSWORD_POSTGRES`**: Password for the PostgreSQL database.

## Spark Session Configuration
- **`spark`**: Initializes a Spark session with configurations for connecting to MinIO (S3A protocol) and Hive Metastore for metadata management. Delta Lake support is enabled through Spark extensions.

## External Functions
The script imports external functions from the `configs` and `functions` modules:

### From `configs`:
- **`configs.tables_postgres_adventureworks`**: A dictionary mapping logical table names to their corresponding PostgreSQL table names.
- **`configs.lake_path['landing_adventure_works']`**: The base path for storing processed tables in the landing zone.

### From `functions` (aliased as `F`):
- **`F.convert_table_name(table_input_name)`**: Converts a PostgreSQL table name into a format suitable for use in the data lake.
- **`F.add_metadata(df_input_data)`**: Adds metadata, such as processing timestamps, to the DataFrame.
- **`F.add_month_key(df_with_update_date, 'modifieddate')`**: Adds a `month_key` column based on the `modifieddate` column to the DataFrame for partitioning.

## Functions

### `process_table(spark, table_input_name)`
Processes a table by extracting data from PostgreSQL, adding metadata and month key, and saving it to the landing zone.
- **Parameters**:
  - **`spark`**: The SparkSession instance.
  - **`table_input_name`**: Name of the table in PostgreSQL to be processed.
- **Behavior**:
  - **Table Path Conversion**: Converts the table name for the data lake using `F.convert_table_name`.
  - **Data Extraction**: Reads data from PostgreSQL using Spark’s JDBC connector.
  - **Add Metadata**: Adds metadata to the DataFrame using `F.add_metadata`.
  - **Add Month Key**: Adds a `month_key` column for partitioning using `F.add_month_key`.
  - **Save Data**: Saves the DataFrame in Parquet format to the landing zone, partitioned by `month_key`.
  - **Error Handling**: Logs success or error messages.

## Main Logic

### 1. Initialization:
- **Spark Session**: Creates a Spark session with configurations for MinIO and Hive Metastore, and enables Delta Lake support.

- **Logging**: Configures logging to record informational messages and errors.

### 2. Processing Tables:
- **Loop Through Tables**: Iterates over the tables defined in `configs.tables_postgres_adventureworks`.
  - **Table Path Conversion**: Converts each table name to a format suitable for the data lake using `F.convert_table_name`.
  - **Data Extraction**: Extracts data from PostgreSQL for each table using Spark’s JDBC connector.
  - **Add Metadata and Month Key**: Adds metadata and a `month_key` to the DataFrame using `F.add_metadata` and `F.add_month_key`.
  - **Save Data**: Saves the processed DataFrame in Parquet format to the landing zone, partitioned by `month_key`.
  - **Error Handling**: Logs success or error messages for each table.

### 3. Completion:
- Logs a message indicating that the ingestion to the landing zone is completed successfully.
