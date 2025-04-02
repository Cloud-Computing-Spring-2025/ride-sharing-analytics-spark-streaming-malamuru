
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("task1") \
    .getOrCreate()

# 2. Define JSON schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())  # Will convert to timestamp later

# 3. Read from socket
raw_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 4. Parse JSON
parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# 5. Print parsed data
query = parsed_df.writeStream \
    .format("csv") \
    .option("header","true")\
    .option("path","output/task1_csv")\
    .option("checkpointLocation","checkpoint/task1")\
    .outputMode("append") \
    .start()

query.awaitTermination()

