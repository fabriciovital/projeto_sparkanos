# Overview
This script is designed to process data from the bronze layer (in Delta format) and convert it to the silver layer (also in Delta format) using Apache Spark. It performs data processing and partitioning by month key, and logs the success or failure of each operation.

## Libraries

- **`pyspark`**: Core library for Apache Spark, providing the SparkSession and DataFrame functionalities.
- **`logging`**: Standard Python library for logging messages during the script execution.
- **`datetime`**: Standard Python library for working with dates and times (though not directly used in this script).
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
- **`spark`**: Initializes a Spark session with configurations to interface with MinIO using the S3A protocol, and Hive Metastore for metadata management. Delta Lake is enabled through Spark extensions.

## External Functions
The script imports external functions from the `configs` and `functions` modules:

### From `configs`:
- **`configs.prefix_layer_name['1']`**: Prefix used for the bronze layer.
- **`configs.lake_path['bronze']`**: The base path for reading Delta tables from the bronze layer.
- **`configs.prefix_layer_name['2']`**: Prefix used for the silver layer.
- **`configs.lake_path['silver']`**: The base path for storing processed Delta tables in the silver layer.
- **`configs.tables_silver`**: A dictionary mapping logical table names to their corresponding SQL queries for processing.

### From `functions` (aliased as `F`):
- **`F.convert_table_name(table_name)`**: Converts a logical table name to a format suitable for use in the data lake.
- **`F.get_query(table_name, input_path, input_prefix_layer_name, tables_silver)`**: Generates a SQL query to read from the bronze layer and convert it to the silver layer.
- **`F.add_metadata(df_input_data)`**: Adds metadata, such as processing timestamps, to the DataFrame.

## Functions

### `process_table(spark, query_input, output_path)`
Processes a DataFrame by executing a SQL query, adding metadata, and saving it in Delta format.
- **Parameters**:
  - **`spark`**: The SparkSession instance.
  - **`query_input`**: SQL query to load data from the bronze layer.
  - **`output_path`**: The path where the processed data will be saved in the silver layer.
- **Behavior**:
  - Executes the SQL query to obtain the DataFrame.
  - Adds metadata using `F.add_metadata`.
  - Saves the DataFrame in Delta format, partitioned by `month_key`.
  - Logs success or error messages.

## Main Logic

### 1. Initialization:
- **Spark Session**: A Spark session is created with configurations for MinIO and Hive Metastore, and Delta Lake is enabled.

- **Logging**: Logging is configured to record informational messages and errors throughout the process.

### 2. Processing Tables:
- **Prefix and Paths**:
  - **`input_prefix_layer_name`**: Prefix for the bronze layer.
  - **`input_path`**: Base path for reading Delta tables from the bronze layer.
  - **`output_prefix_layer_name`**: Prefix for the silver layer.
  - **`output_path`**: Base path for storing processed Delta tables in the silver layer.

- **Processing Loop**:
  - Iterates over the tables defined in `configs.tables_silver`.
  - For each table:
    - **Table Name Conversion**: The table name is converted to a format suitable for the data lake using `F.convert_table_name`.
    - **Query Generation**: A SQL query is generated to read from the bronze layer using `F.get_query`.
    - **Data Processing**: Calls `process_table` to execute the query, add metadata, and save the data in Delta format.
  - Logs a completion message or error if any issue occurs.

### 3. Completion:
- After all tables are processed, a log message indicates that the processing from bronze to silver has been completed successfully.
