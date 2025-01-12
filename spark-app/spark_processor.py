import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum as spark_sum, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://admin:password123@localhost:27017/')
SPARK_MASTER = os.getenv('SPARK_MASTER_URL', 'local[*]')

# Define schema for incoming events
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("page", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("element", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("source", StringType(), True),
    StructField("country", StringType(), True),
])

def create_spark_session():
    """Create Spark session with MongoDB connector"""
    print(" Creating Spark Session...")
    
    spark = SparkSession.builder \
        .appName("DistributedDataProcessing") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(" Spark Session created successfully")
    
    return spark

def process_stream(spark):
    """Process Kafka stream and write to MongoDB"""
    
    print(f"📡 Reading from Kafka topic 'user-events' at {KAFKA_BROKER}")
    
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "user-events") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    events_df = df.select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")
    
    # Add watermark for event time processing
    events_with_time = events_df \
        .withColumn("event_time", col("timestamp").cast("timestamp")) \
        .withWatermark("event_time", "10 minutes")
    
    print(" Kafka stream connected")
    
    # --- Analytics 1: Event counts by type (5-minute windows) ---
    event_counts = events_with_time \
        .groupBy(
            window("event_time", "5 minutes"),
            "event_type"
        ) \
        .agg(count("*").alias("count")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "event_type",
            "count"
        )
    
    # Write to MongoDB
    query1 = event_counts \
        .writeStream \
        .outputMode("update") \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/checkpoint1") \
        .option("spark.mongodb.database", "analytics") \
        .option("spark.mongodb.collection", "event_counts") \
        .start()
    
    print("📊 Started writing event_counts to MongoDB")
    
    # --- Analytics 2: Purchase analytics ---
    purchase_events = events_with_time \
        .filter(col("event_type") == "purchase")
    
    purchase_analytics = purchase_events \
        .groupBy(
            window("event_time", "5 minutes"),
            "user_id"
        ) \
        .agg(
            count("*").alias("purchase_count"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "user_id",
            "purchase_count",
            "total_amount",
            "avg_amount"
        )
    
    query2 = purchase_analytics \
        .writeStream \
        .outputMode("update") \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/checkpoint2") \
        .option("spark.mongodb.database", "analytics") \
        .option("spark.mongodb.collection", "purchase_analytics") \
        .start()
    
    print("💰 Started writing purchase_analytics to MongoDB")
    
    # --- Analytics 3: User activity by country (for signups) ---
    signup_events = events_with_time \
        .filter(col("event_type") == "signup")
    
    signup_by_country = signup_events \
        .groupBy(
            window("event_time", "5 minutes"),
            "country",
            "source"
        ) \
        .agg(count("*").alias("signup_count")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "country",
            "source",
            "signup_count"
        )
    
    query3 = signup_by_country \
        .writeStream \
        .outputMode("update") \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/checkpoint3") \
        .option("spark.mongodb.database", "analytics") \
        .option("spark.mongodb.collection", "signup_analytics") \
        .start()
    
    print(" Started writing signup_analytics to MongoDB")
    
    # --- Raw events storage ---
    query4 = events_with_time \
        .writeStream \
        .outputMode("append") \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/checkpoint4") \
        .option("spark.mongodb.database", "analytics") \
        .option("spark.mongodb.collection", "raw_events") \
        .start()
    
    print(" Started writing raw_events to MongoDB")
    
    print("\n" + "="*60)
    print(" SPARK STREAMING IS NOW RUNNING")
    print("="*60)
    print(" Processing events from Kafka and writing to MongoDB:")
    print("   - Event counts by type (5-min windows)")
    print("   - Purchase analytics (revenue, avg purchase)")
    print("   - Signup analytics by country and source")
    print("   - Raw events storage")
    print("="*60 + "\n")
    
    # Wait for all queries
    query1.awaitTermination()

def main():
    print("\n" + "="*60)
    print(" DISTRIBUTED DATA PROCESSING PLATFORM")
    print("="*60 + "\n")
    
    # Wait for services to be ready
    print(" Waiting for Kafka and MongoDB to be ready...")
    time.sleep(30)
    
    spark = create_spark_session()
    
    try:
        process_stream(spark)
    except KeyboardInterrupt:
        print("\n  Stopping Spark application...")
    finally:
        spark.stop()
        print(" Spark application stopped")

if __name__ == '__main__':
    main()