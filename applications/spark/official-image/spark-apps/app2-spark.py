from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

conf = SparkConf()

conf.setAppName("Sample write Delta table")
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
conf.set("spark.hadoop.fs.s3a.access.key", "chapolin")
conf.set("spark.hadoop.fs.s3a.secret.key", "mudar@123")
conf.set("spark.hadoop.fs.s3a.path.style.access", True)
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
conf.set("hive.metastore.uris", "thrift://metastore:9083")

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

data2 = [("James", "Smith", "M", 3000),
         ("Michael", "Rose", "M", 6000),
         ("Robert", "Willians", "M", 5500),
         ("Maria", "Anne", "F", 7000)
        ]

schema = StructType([
    StructField("firtsname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", StringType(), True)
])


df = spark.createDataFrame(data=data2, schema=schema)

df.show()

df.write.format("delta").mode("append").save('s3a://bronze/tb_wallace')