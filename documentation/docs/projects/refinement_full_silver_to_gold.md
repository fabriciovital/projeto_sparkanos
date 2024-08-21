# Overview
This script is designed to refine data from the silver layer to the gold layer using Apache Spark. It involves executing SQL queries to transform data, adding metadata, and saving the results in Delta format with partitioning by month key. The script logs the progress and any errors encountered during the processing.

## Libraries

- **`pyspark`**: Core library for Apache Spark, providing functionalities such as `SparkSession` and `DataFrame` operations.
- **`logging`**: Standard Python library for logging messages during script execution.
- **`datetime`**: Standard Python library for handling dates and times (though not directly used in this script).
- **`dotenv`**: Library to load environment variables from a `.env` file.
- **`os`**: Standard Python library for interacting with the operating system and environment variables.
- **`configs`**: Custom module containing configuration settings used by the script.
- **`functions`**: Custom module (aliased as `F`) containing external functions for data processing.

## Variables

### Environment Variables
These variables are loaded from a `.env` file using `dotenv` and are used for configuring the Spark session and access credentials:

- **`HOST_ADDRESS`**: The address of the host machine where MinIO is running.
- **`MINIO_ACCESS_KEY`**: Access key for MinIO.
- **`MINIO_SECRET_KEY`**: Secret key for MinIO.

## Spark Session Configuration
- **`spark`**: Initializes a Spark session with configurations for MinIO (S3A protocol) and Hive Metastore for metadata management. Delta Lake support is enabled through Spark extensions.

## External Functions
The script imports external functions from the `configs` and `functions` modules:

### From `configs`:
- **`configs.prefix_layer_name['2']`**: Prefix used for the silver layer.
- **`configs.lake_path['silver']`**: Base path for reading Delta tables from the silver layer.
- **`configs.prefix_layer_name['3']`**: Prefix used for the gold layer.
- **`configs.lake_path['gold']`**: Base path for storing processed Delta tables in the gold layer.
- **`configs.tables_gold`**: A dictionary mapping logical table names to their corresponding SQL queries for processing.

### From `functions` (aliased as `F`):
- **`F.convert_table_name(table_name)`**: Converts a logical table name to a format suitable for use in the data lake.
- **`F.get_query(table_name, input_path, input_prefix_layer_name, tables_gold)`**: Generates a SQL query to read from the silver layer and convert it to the gold layer.
- **`F.add_metadata(df_input_data)`**: Adds metadata, such as processing timestamps, to the DataFrame.

## Functions

### `process_table(spark, query_input, output_path)`
Processes a DataFrame by executing a SQL query, adding metadata, and saving it in Delta format.
- **Parameters**:
  - **`spark`**: The SparkSession instance.
  - **`query_input`**: SQL query to load data from the silver layer.
  - **`output_path`**: Path where the processed data will be saved in the gold layer.
- **Behavior**:
  - Executes the SQL query to obtain the DataFrame.
  - Adds metadata using `F.add_metadata`.
  - Saves the DataFrame in Delta format, partitioned by `month_key`.
  - Logs success or error messages.

## Main Logic

### 1. Initialization:
- **Spark Session**: Creates a Spark session with configurations for MinIO and Hive Metastore, and enables Delta Lake support.

- **Logging**: Configures logging to record informational messages and errors.

### 2. Processing Tables:
- **Prefix and Paths**:
  - **`input_prefix_layer_name`**: Prefix for the silver layer.
  - **`input_path`**: Base path for reading Delta tables from the silver layer.
  - **`output_prefix_layer_name`**: Prefix for the gold layer.
  - **`output_path`**: Base path for storing processed Delta tables in the gold layer.

- **Processing Loop**:
  - Iterates over the tables defined in `configs.tables_gold`.
  - For each table:
    - **Table Name Conversion**: Converts the table name to a format suitable for the data lake using `F.convert_table_name`.
    - **Query Generation**: Generates a SQL query to read from the silver layer using `F.get_query`.
    - **Data Processing**: Calls `process_table` to execute the query, add metadata, and save the data in Delta format.
  - Logs a completion message or error if any issue occurs.

### 3. Completion:
- After processing all tables, logs a message indicating that the refinement to the gold layer is completed successfully.
