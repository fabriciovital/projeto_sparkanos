from datetime import datetime

import pyspark
from pyspark.sql.functions import date_format, lit, unix_timestamp
from pyspark.sql.types import DoubleType, IntegerType, TimestampType


def convert_table_name(table_name):
    return table_name.replace(".", "_")


def add_metadata(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    df_with_metadata = df.withColumn("last_update", lit(datetime.now()))
    return df_with_metadata


def add_month_key(df, date_column_name):
    df_with_month_key = df.withColumn(
        "month_key", date_format(df[date_column_name], "yyyyMM").cast(IntegerType())
    )
    return df_with_month_key
