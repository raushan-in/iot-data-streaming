'''
Consumes messages from Kafka, processes in PySpark, and writes to InfluxDB.
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType
from influxdb_client import InfluxDBClient, Point, WriteOptions
from config import KAFKA_BROKER, KAFKA_TOPIC, influx_bucket, influx_org, influx_token, influx_url

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("IoT-Stream-to-InfluxDB") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Step 2: Define schema for incoming JSON data
schema = StructType() \
    .add("uid", IntegerType()) \
    .add("zone_code", StringType()) \
    .add("location_ping", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("purchase_code", StringType())

# Step 3: Connect and Read data from Kafka Stream
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Step 4: Parse JSON and transform data
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Step 5: Apply transformations and aggregation
aggregated_stream = parsed_stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "10 minutes"),
        col("zone_code"),
        col("location_ping")
    ) \
    .count() \
    .select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        col("zone_code"),
        col("location_ping"),
        col("count").alias("ping_count")
    )

# Step 6: Write to InfluxDB
def write_to_influxdb(row):
    with InfluxDBClient(url=influx_url, token=influx_token, org=influx_org) as client:
        write_api = client.write_api(write_options=WriteOptions(batch_size=1))
        point = Point("iot_metrics") \
            .tag("zone_code", row.zone_code) \
            .tag("location_ping", row.location_ping) \
            .field("ping_count", row.ping_count) \
            .time(row.start_time)
        write_api.write(bucket=influx_bucket, record=point)

# Use foreachBatch to write streaming data to InfluxDB
query = aggregated_stream.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch_id: df.foreach(write_to_influxdb)) \
    .start()

query.awaitTermination()
