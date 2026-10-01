from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("toMinio") \
    .getOrCreate()

spark.conf.set(
    "spark.hadoop.fs.s3a.endpoint",
    "http://minio:9000"
)

spark.conf.set(
    "spark.hadoop.fs.s3a.path.style.access",
    "true"
)

spark.conf.set(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider"
)

df = spark.read.parquet(
    "/opt/airflow/data/yellow_tripdata_2026-*.parquet"
)

df.write \
    .mode("overwrite") \
    .parquet(
        "s3a://nyctaxidata/raw_data/"
    )

spark.stop()