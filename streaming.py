import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, TimestampType, LongType
from dotenv import load_dotenv

# Tải biến môi trường
load_dotenv()
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:29092")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadminpassword")

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell'

spark = SparkSession.builder \
    .appName("PostgresCDC_Streaming_To_MinIO") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", "pgstream_v3.public.orders") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS STRING)")

# 1. Nhận dữ liệu THÔ từ Kafka
order_schema = StructType() \
    .add("id", IntegerType()) \
    .add("order_date", LongType()) \
    .add("customer_name", StringType()) \
    .add("product_name", StringType()) \
    .add("price", StringType()) \
    .add("quantity", IntegerType()) \
    .add("status", StringType())

debezium_schema = StructType().add("payload", StructType().add("after", order_schema))

raw_parsed_df = value_df \
    .select(from_json(col("value"), debezium_schema).alias("data")) \
    .select("data.payload.after.*") \
    .filter(col("id").isNotNull())

# 2. TRANSFORM Cực mạnh: Chống lệch múi giờ và xử lý lỗi chữ số
final_df = raw_parsed_df \
    .withColumn("price", regexp_replace(col("price"), ",", "").cast(DoubleType())) \
    .withColumn("order_date", (col("order_date") / 1000000).cast(TimestampType()))

# 3. Ghi dữ liệu Parquet xuống MinIO
query = final_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://lakehouse/orders/") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/orders/") \
    .start()

query.awaitTermination()