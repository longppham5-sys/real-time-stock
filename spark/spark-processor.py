from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, timestamp_seconds, window,
    min_by, max_by, max, min, when, abs,
    sum as spark_sum, avg
)
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
import os

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("CryptoDownsamplingProcessor") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "2")
CA_CERT_PATH = "/etc/cluster-ca/ca.crt"
USER_CERT_PATH = "/etc/consumer-credentials/user.crt"
USER_KEY_PATH = "/etc/consumer-credentials/user.key"

# 2. ĐN Schema
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("quantity", DoubleType()) \
    .add("timestamp", LongType())

# 3. Đọc Stream từ Kafka
def read_file(path):
    with open(path, "r") as f:
        return f.read()

ca_cert = read_file(CA_CERT_PATH)
user_cert = read_file(USER_CERT_PATH)
user_key = read_file(USER_KEY_PATH)

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "my-cluster-kafka-bootstrap.default.svc:9093") \
    .option("subscribe", "crypto-prices") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.type", "PEM") \
    .option("kafka.ssl.truststore.certificates", ca_cert) \
    .option("kafka.ssl.keystore.type", "PEM") \
    .option("kafka.ssl.keystore.certificate.chain", user_cert) \
    .option("kafka.ssl.keystore.key", user_key) \
    .load()

# 4. Parse dữ liệu
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", timestamp_seconds(col("timestamp") / 1000)) \
    .withColumn("price_volume", col("price") * col("quantity"))

# 5. Windowing & Tổng hợp OHLC - 10s
windowed_df = parsed_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("symbol")
    ) \
    .agg(
        min_by("price", "event_time").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        max_by("price", "event_time").alias("close"),
        spark_sum("quantity").alias("volume"),
        avg("price").alias("avg_price"),
        when(
            spark_sum("quantity") != 0,
            spark_sum("price_volume") / spark_sum("quantity")
        ).otherwise(None).alias("vwap"),
        max("event_time").alias("event_time")
    ) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end"))

# 6. chỉ báo bổ sung
df_with_indicators = windowed_df \
    .withColumn(
        "price_change",
        col("close") - col("open")
    ) \
    .withColumn(
        "price_change_pct",
        when(
            col("open") != 0,
            ((col("close") - col("open")) / col("open")) * 100
        ).otherwise(0)
    ) \
    .withColumn(
        "volatility",
        when(
            col("open") != 0,
            ((col("high") - col("low")) / col("open")) * 100
        ).otherwise(0)
    ) \
    .withColumn(
        "trend",
        when(col("close") > col("open"), "UP")
        .when(col("close") < col("open"), "DOWN")
        .otherwise("SIDEWAYS")
    ) \
    .withColumn(
        "body_size",
        abs(col("close") - col("open"))
    ) \
    .withColumn(
        "range_size",
        col("high") - col("low")
    ) \
    .withColumn(
        "candle_strength",
        when(
            col("range_size") != 0,
            col("body_size") / col("range_size")
        ).otherwise(0)
    )

# 7. RSI Strategy
df_with_rsi = df_with_indicators \
    .withColumn("diff", col("close") - col("open")) \
    .withColumn(
        "gain",
        when(col("diff") > 0, col("diff")).otherwise(0.001)
    ) \
    .withColumn(
        "loss",
        when(col("diff") < 0, abs(col("diff"))).otherwise(0.001)
    ) \
    .withColumn(
        "rs",
        col("gain") / col("loss")
    ) \
    .withColumn(
        "rsi",
        100 - (100 / (1 + col("rs")))
    )

# 8. Đánh giá mức độ rủi ro
df_with_risk = df_with_rsi \
    .withColumn(
        "risk_level",
        when(col("volatility") >= 2, "HIGH")
        .when(col("volatility") >= 1, "MEDIUM")
        .otherwise("LOW")
    )

# 9. Trading Advice
final_df = df_with_risk.withColumn(
    "advice",
    when(
        (col("rsi") <= 30) &
        (col("trend") == "UP") &
        (col("risk_level") != "HIGH"),
        "STRONG BUY (Oversold + Uptrend)"
    )
    .when(
        (col("rsi") <= 30) &
        (col("risk_level") == "HIGH"),
        "WATCH BUY - HIGH RISK"
    )
    .when(
        (col("rsi") >= 70) &
        (col("trend") == "DOWN"),
        "STRONG SELL (Overbought + Downtrend)"
    )
    .when(
        (col("rsi") >= 70),
        "SELL WARNING (Overbought)"
    )
    .when(
        (col("volatility") >= 2),
        "HOLD - HIGH VOLATILITY"
    )
    .when(
        (col("close") > col("vwap")) &
        (col("trend") == "UP"),
        "BUY SIGNAL (Above VWAP)"
    )
    .when(
        (col("close") < col("vwap")) &
        (col("trend") == "DOWN"),
        "SELL SIGNAL (Below VWAP)"
    )
    .otherwise("HOLD (Neutral)")
).select(
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "avg_price",
    "vwap",
    "price_change",
    "price_change_pct",
    "volatility",
    "trend",
    "candle_strength",
    "rsi",
    "risk_level",
    "advice",
    "event_time",
    "window_start",
    "window_end"
)

# 10. Cấu hình MongoDB
MONGO_USER = "databaseAdmin"
MONGO_PASS = "databaseAdmin123456"
MONGO_HOST = "mongodb-rs0.default.svc.cluster.local"
MONGO_DB = "crypto"
MONGO_COLLECTION = "prices_downsampled"
MONGO_CA_CERT_PATH = "/etc/mongo-certs/ca.crt" 
MONGO_CLIENT_CERT_PATH = "/tmp/certs/mongo.pem"
mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:27017/{MONGO_DB}.{MONGO_COLLECTION}" \
            f"?authSource=admin&tls=true"

# 11. Ghi dữ liệu vào MongoDB
query = final_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "hdfs://my-hadoop-hadoop-hdfs-nn:9000/user/long/checkpoints/") \
    .option("spark.mongodb.connection.uri", mongo_uri) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()