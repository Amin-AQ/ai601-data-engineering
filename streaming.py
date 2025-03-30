from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json, count, desc, row_number, avg, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.window import Window
from prometheus_client import Gauge, start_http_server

start_http_server(8080)

spark = SparkSession.builder.appName("TrafficData").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def read_kafka():
    return spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "traffic_data") \
        .option("startingOffsets", "latest") \
        .load()

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", FloatType(), True),
    StructField("congestion_level", StringType(), True)
])

def parse_events(kafka_df):
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
    return parsed_df.select("data.*")

top_congested_gauge = Gauge("top_congested_sensors", "Top congested sensors based on vehicle count", ["sensor_id", "congestion_level"])

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"=== Batch {batch_id}: No data received ===")
        return

    print(f"=== Batch {batch_id}: Processing data ===")
    batch_df.printSchema()

    aggregated_df = batch_df.groupBy(
        window(col("timestamp"), "1 minute"), col("sensor_id"), col("congestion_level")
    ).agg(count("*").alias("event_count"))  

    w = Window.partitionBy("congestion_level", "window").orderBy(desc("event_count"))

    top_congested = aggregated_df.withColumn("rn", row_number().over(w)).filter(col("rn") <= 3)

    for row in top_congested.collect():  
        sensor_id = row['sensor_id']
        congestion_level = row['congestion_level']
        count_value = row['event_count']
        print(f"Sending to Prometheus - Sensor: {sensor_id}, Count: {count_value}, Congestion Level: {congestion_level}")
        top_congested_gauge.labels(sensor_id=sensor_id, congestion_level=congestion_level).set(count_value)

def write_to_kafka(df, checkpoint_path):
    return df \
        .selectExpr("CAST(sensor_id AS STRING) AS key", "CAST(to_json(struct(*)) AS STRING) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "traffic_analysis") \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .start()

def main():
    kafka_df = read_kafka()
    events_df = parse_events(kafka_df)
    events_df = events_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    
    traffic_volume_df = events_df \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("sensor_id")
        ) \
        .agg(
            count("*").alias("event_count"),
            avg("vehicle_count").alias("avg_vehicle_count"),
            avg("average_speed").alias("avg_speed")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor_id"),
            col("event_count"),
            col("avg_vehicle_count"),
            col("avg_speed")
        )
    
    busiest_sensors_df = events_df \
        .withWatermark("timestamp", "30 minutes") \
        .groupBy(
            window(col("timestamp"), "30 minutes"),
            col("sensor_id")
        ) \
        .agg(
            count("*").alias("event_count"),
            avg("vehicle_count").alias("avg_vehicle_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor_id"),
            col("event_count"),
            col("avg_vehicle_count")
        )
    
    congestion_df = events_df \
        .withWatermark("timestamp", "10 minutes") \
        .filter(col("congestion_level") == "HIGH") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("sensor_id")
        ) \
        .count() \
        .withColumnRenamed("count", "high_congestion_count") \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor_id"),
            col("high_congestion_count")
        )
    

    traffic_volume_query = write_to_kafka(traffic_volume_df, "/tmp/checkpoint-traffic-volume")
    busiest_sensors_query = write_to_kafka(busiest_sensors_df, "/tmp/checkpoint-busiest-sensors")
    congestion_query = write_to_kafka(congestion_df, "/tmp/checkpoint-congestion")
    
    metrics_query = events_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .start()
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()