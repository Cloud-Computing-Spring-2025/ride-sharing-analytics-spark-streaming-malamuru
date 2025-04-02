
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg, to_timestamp, window
from pyspark.sql.types import StructType, StringType, DoubleType

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("task2") \
    .getOrCreate()

# 2. Define schema for incoming JSON
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())  # Will convert to timestamp later

# 3. Read streaming data from socket
raw_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 4. Parse the incoming JSON and convert timestamp
parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
parsed_df = parsed_df.withColumn("event_time", to_timestamp("timestamp"))

# 5. Group by driver_id within sliding time windows
agg_df = parsed_df \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        window(col("event_time"), "2 minutes", "1 minute"),  # 2-min window sliding every 1 min
        col("driver_id")
    ) \
    .agg(
        sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )

# 6. Defining a function 
def save_to_csv(batch_df, batch_id):
    # Flatten the 'window' struct column into two columns: window_start, window_end
    batch_df = batch_df.withColumn("window_start", col("window.start")) \
                       .withColumn("window_end", col("window.end")) \
                       .drop("window")  # drop the nested column

    # Now write the flattened DataFrame to CSV
    output_path = f"output/task2_csv/batch_{batch_id}"
    batch_df.write.option("header", "true").mode("overwrite").csv(output_path)

# 7. Write to console for real-time monitoring

query = agg_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(save_to_csv) \
    .option("checkpointLocation", "checkpoint/task2") \
    .start()

query.awaitTermination()


