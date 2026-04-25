from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, timestamp_seconds, window, first, last, max, min, when, abs
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
 
# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("CryptoDownsamplingProcessor") \
    .getOrCreate()
 
# Tối ưu partition
spark.conf.set("spark.sql.shuffle.partitions", "2")
 
# 2. Định nghĩa Schema
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("quantity", DoubleType()) \
    .add("timestamp", LongType())
 
# 3. Đọc Stream từ Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "my-cluster-kafka-bootstrap.default.svc:9092") \
    .option("subscribe", "crypto-prices") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()
 
# 4. Parse dữ liệu
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", timestamp_seconds(col("timestamp") / 1000))
 
# 5. Windowing & Aggregation (OHLC)
windowed_df = parsed_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("symbol")
    ) \
    .agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        max("event_time").alias("event_time")
    )
 
# --- BƯỚC MỚI: TÍCH HỢP RSI STRATEGY ---
# Tính toán mức tăng/giảm (Gain/Loss)
# RS = Average Gain / Average Loss
# Ở mức độ đồ án, ta tính sức mạnh tương đối dựa trên thân nến (Close - Open)
df_with_rsi = windowed_df \
    .withColumn("diff", col("close") - col("open")) \
    .withColumn("gain", when(col("diff") > 0, col("diff")).otherwise(0.001)) \
    .withColumn("loss", when(col("diff") < 0, abs(col("diff"))).otherwise(0.001)) \
    .withColumn("rs", col("gain") / col("loss")) \
    .withColumn("rsi", 100 - (100 / (1 + col("rs"))))
 
# Đưa ra lời khuyên Trading Advice
final_df = df_with_rsi.withColumn("advice",
    when(col("rsi") >= 70, "STRONG SELL (Overbought)")
    .when(col("rsi") <= 30, "STRONG BUY (Oversold)")
    .otherwise("HOLD (Neutral)")
).select(
    "symbol", "open", "high", "low", "close", "event_time", "rsi", "advice"
)
 
# 6. Cấu hình MongoDB
MONGO_USER = "databaseAdmin"
MONGO_PASS = "databaseAdmin123456"
MONGO_HOST = "mongodb-rs0.default.svc.cluster.local"
MONGO_DB = "crypto"
MONGO_COLLECTION = "prices_downsampled"
 
mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:27017/{MONGO_DB}.{MONGO_COLLECTION}?authSource=admin"
 
# 7. Ghi dữ liệu
query = final_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "hdfs://my-hadoop-hadoop-hdfs-nn:9000/user/long/checkpoints/") \
    .option("spark.mongodb.connection.uri", mongo_uri) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .start()
 
query.awaitTermination()