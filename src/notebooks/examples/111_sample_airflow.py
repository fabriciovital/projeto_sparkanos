from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import os

load_dotenv()

HOST_ADDRESS=os.getenv('HOST_ADDRESS')
MINIO_ACCESS_KEY=os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY=os.getenv('MINIO_SECRET_KEY')

# Configuração do Spark
conf = SparkConf()
conf.setAppName("Sample Airflow")
conf.set("spark.hadoop.fs.s3a.endpoint", f"http://{HOST_ADDRESS}:9000")
conf.set("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
conf.set("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
conf.set("spark.hadoop.fs.s3a.path.style.access", True)
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
)
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set(
    "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
)
conf.set("hive.metastore.uris", "thrift://metastore:9083")

# Inicialização da sessão do Spark
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

# Dados de exemplo
data2 = [
    ("James", "", "Smith", "36636", "M", 3000),
    ("Michael", "Rose", "", "40288", "M", 4000),
    ("Robert", "", "Williams", "42114", "M", 4000),
    ("Maria", "Anne", "Jones", "39192", "F", 4000),
    ("Jen", "Mary", "Brown", "", "F", -1),
]

# Esquema dos dados
schema = StructType(
    [
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("id", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", IntegerType(), True),
    ]
)

# Criando DataFrame
df = spark.createDataFrame(data=data2, schema=schema)


df.write.format("delta").mode("append").save("s3a://bronze/airflow_table")
