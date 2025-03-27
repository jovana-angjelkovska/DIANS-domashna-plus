from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Kafka and Spark configurations
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "stock-data"

# Initialize Spark session with Structured Streaming
spark = SparkSession.builder \
    .appName("RealTimeProcessingService") \
    .master("local[*]") \
    .getOrCreate()

# Define the schema of incoming Kafka messages
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", FloatType(), True)
])

# Read data from Kafka topic as a streaming DataFrame
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize JSON messages from Kafka
processed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.timestamp").cast(TimestampType()).alias("timestamp"),
        col("data.symbol"),
        col("data.open"),
        col("data.high"),
        col("data.low"),
        col("data.close"),
        col("data.volume")
    )

# Perform transformations (e.g., calculate moving average of 'close' price)
aggregated_stream = processed_stream.groupBy(
    window(col("timestamp"), "5 minutes"),  # 5-minute window for aggregation
    col("symbol")
).agg(
    avg(col("close")).alias("average_close_price")
)

# Write the processed stream to the console (or another sink like Kafka or a database)
query = aggregated_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
