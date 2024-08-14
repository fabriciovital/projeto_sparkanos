from datetime import datetime

from pyspark.sql.functions import date_format, lit, unix_timestamp
from pyspark.sql.types import IntegerType


def convert_table_name(table_name):
    """Function to convert table name, replacing . with _"""
    return table_name.replace(".", "_")


def add_metadata(df):
    """
    Add metadata columns to the DataFrame.

    Parameters:
        df (pyspark.sql.DataFrame): Input DataFrame.

    Returns:
        pyspark.sql.DataFrame: DataFrame with metadata columns added.
    """
    df_with_metadata = df.withColumn("last_update", lit(datetime.now()))
    return df_with_metadata


def add_month_key_column(df, date_column_name):
    """
    Add a month key column to the DataFrame using the year and month from the specified date column.

    Parameters:
        df (pyspark.sql.DataFrame): Input DataFrame.
        date_column_name (str): Name of the date column.

    Returns:
        pyspark.sql.DataFrame: DataFrame with month key column added.
    """
    df_with_month_key = df.withColumn(
        "month_key", date_format(df[date_column_name], "yyyyMM").cast(IntegerType())
    )
    return df_with_month_key
