import os
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.2.0 pyspark-shell'

# 1. Setup SparkSession giống với streaming job (kèm các config Delta)
spark = SparkSession.builder \
    .appName("CompactionJob") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Trỏ đến đường dẫn bảng Delta
table_path = "s3a://lakehouse/orders/"

# 3. Lệnh "Thần thánh" để gộp file
deltaTable = DeltaTable.forPath(spark, table_path)
deltaTable.optimize().executeCompaction()

print("Compaction thành công!")
spark.stop()