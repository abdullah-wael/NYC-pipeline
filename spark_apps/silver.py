from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, abs
spark = SparkSession.builder \
    .appName("readFromMinio") \
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
    "s3a://nyctaxidata/raw_data/*.parquet"
)





lookup = spark.read.csv(
    "/opt/airflow/data/taxi_zone_lookup.csv",
    header=True,
    inferSchema=True
)

df2 = df.withColumn(
    "RatecodeID",
    when(col("RatecodeID").isNull(), 99)
    .otherwise(col("RatecodeID"))
)

df3 = df2.withColumn(
    "payment_type",
    when((col("payment_type") < 0) | (col("payment_type") > 6), 5)
    .otherwise(col("payment_type"))
)

df4 = df3.withColumn(
    "store_and_fwd_flag",
    when(col("store_and_fwd_flag").isNull(), "UNKNOWN")
    .otherwise(col("store_and_fwd_flag"))
)

dfRejected = df4.filter(
    col("tpep_pickup_datetime").isNull()
    | col("tpep_dropoff_datetime").isNull()
    | col("fare_amount").isNull()
    | col("total_amount").isNull()
    | (col("tpep_pickup_datetime") > col("tpep_dropoff_datetime"))
    | (col("trip_distance") < 0)
    | (col("fare_amount") < 0)
    | (col("tip_amount") < 0)
    | (col("total_amount") < 0)
    | (col("Airport_fee") < 0)
    | ((col("payment_type") != 1) & (col("tip_amount") > 0))
)

df5 = df4.filter(
    col("tpep_pickup_datetime").isNotNull()
    & col("tpep_dropoff_datetime").isNotNull()
    & col("fare_amount").isNotNull()
    & col("total_amount").isNotNull()
    & (col("tpep_pickup_datetime") <= col("tpep_dropoff_datetime"))
    & (col("trip_distance") >= 0)
    & (col("fare_amount") >= 0)
    & (col("tip_amount") >= 0)
    & (col("total_amount") >= 0)
    & (col("Airport_fee") >= 0)
    & ~((col("payment_type") != 1) & (col("tip_amount") > 0))
)

df6 = df5.withColumn(
    "recalculated_total_amount",
    col("fare_amount")
    + col("tip_amount")
    + col("tolls_amount")
    + col("Airport_fee")
    + col("congestion_surcharge")
    + col("improvement_surcharge")
    + col("extra")
    + col("mta_tax")
)

df7 = df6.withColumn(
    "diff_between_recalculated_and_total",
    col("recalculated_total_amount") - col("total_amount")
)



df7.write.parquet(
    "s3a://nyctaxidata/transformed/",
    mode="overwrite"
)
df_locations=spark.read.csv(
    "/opt/airflow/data/taxi_zone_lookup.csv",
    header=True,
    inferSchema=True
)
df_locations.write.parquet(
    "s3a://nyctaxidata/lookup/",
    mode="overwrite"
)

dfRejected.write.parquet(
    "s3a://nyctaxidata/rejected/",
    mode="overwrite"
)

import os
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

df8 = df7.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType()))
df8 = df7.withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType()))


# sfOptions = {
#     "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
#     "sfUser": os.environ["SNOWFLAKE_USER"],
#     "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
#     "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
#     "sfSchema": os.environ["SNOWFLAKE_SCHEMA"],
#     "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
#     "sfRole": os.environ["SNOWFLAKE_ROLE"],
# }



# df8.write \
#     .format("net.snowflake.spark.snowflake") \
#     .options(**sfOptions) \
#     .option("dbtable", "TRANSFORMED_TRIPS") \
#     .mode("overwrite") \
#     .save()

# df_locations.write \
#     .format("net.snowflake.spark.snowflake") \
#     .options(**sfOptions) \
#     .option("dbtable", "TAXI_ZONE_LOOKUP") \
#     .mode("overwrite") \
#     .save()