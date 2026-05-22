from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, timestamp_seconds, window,
    min_by, max_by, max, min, when, abs,
    sum as spark_sum, avg, stddev, lag, count, expr
)
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
from pyspark.sql.window import Window
import os

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("CryptoStreaming_BB20_SMA_Crossover") \
    .getOrCreate()

# Tối ưu partition cho môi trường nhỏ
spark.conf.set("spark.sql.shuffle.partitions", "2")

# Đọc đường dẫn Cert từ Environment Variables
CA_CERT_PATH = "/etc/cluster-ca/ca.crt"
USER_CERT_PATH = "/etc/consumer-credentials/user.crt"
USER_KEY_PATH = "/etc/consumer-credentials/user.key"

# 2. Định nghĩa Schema
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("quantity", DoubleType()) \
    .add("timestamp", LongType())

# 3. Đọc Stream từ Kafka với cấu hình mTLS
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

# 5. Windowing & Aggregation OHLC - 10s
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

# 6. Tính toán các chỉ báo nến cơ bản
df_with_indicators = windowed_df \
    .withColumn("price_change", col("close") - col("open")) \
    .withColumn(
        "price_change_pct",
        when(col("open") != 0, ((col("close") - col("open")) / col("open")) * 100).otherwise(0)
    ) \
    .withColumn(
        "volatility",
        when(col("open") != 0, ((col("high") - col("low")) / col("open")) * 100).otherwise(0)
    ) \
    .withColumn(
        "trend",
        when(col("close") > col("open"), "UP")
        .when(col("close") < col("open"), "DOWN")
        .otherwise("SIDEWAYS")
    ) \
    .withColumn("body_size", abs(col("close") - col("open"))) \
    .withColumn("range_size", col("high") - col("low")) \
    .withColumn(
        "candle_strength",
        when(col("range_size") != 0, col("body_size") / col("range_size")).otherwise(0)
    )

ohlc_df = df_with_indicators.select(
    "symbol", "open", "high", "low", "close", "volume",
    "avg_price", "vwap", "price_change", "price_change_pct",
    "volatility", "trend", "candle_strength", "event_time",
    "window_start", "window_end"
)

# 7. Cấu hình MongoDB
MONGO_USER = "databaseAdmin"
MONGO_PASS = "databaseAdmin123456"
MONGO_HOST = "mongodb-rs0.default.svc.cluster.local"
MONGO_DB = "crypto"

ohlc_uri = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:27017/{MONGO_DB}.prices_downsampled"
    f"?authSource=admin&tls=true"
)

indicator_uri = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:27017/{MONGO_DB}.prices_indicators_10s"
    f"?authSource=admin&tls=true"
)

# 8. Hàm foreachBatch: Tính toán BB20 + SMA Crossover dựa trên Lịch sử
def process_batch(batch_df, batch_id):
    print(f"Processing batch_id = {batch_id}")

    if batch_df.rdd.isEmpty():
        print("Empty batch, skip")
        return

    # 1. Ghi OHLC mới xuống Database
    batch_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("spark.mongodb.connection.uri", ohlc_uri) \
        .save()

    # 2. Đọc 30 phút dữ liệu gần nhất để tính Rolling Indicators
    recent_df = spark.read \
        .format("mongodb") \
        .option("spark.mongodb.connection.uri", ohlc_uri) \
        .load() \
        .filter(
            (col("symbol") == "BTCUSDT") &
            col("close").isNotNull() &
            col("event_time").isNotNull() &
            (col("event_time") >= expr("current_timestamp() - INTERVAL 30 MINUTES"))
        )

    # --- ĐỊNH NGHĨA CÁC CỬA SỔ (WINDOWS) ---
    w = Window.partitionBy("symbol").orderBy("event_time")
    
    bb_w = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-19, 0) # BB 20 nến
    sma_short_w = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-8, 0) # SMA 9 nến
    sma_long_w = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-20, 0) # SMA 21 nến
    rsi_w = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-13, 0) # RSI 14 nến (14 chu kỳ)

    # --- TÍNH TOÁN CHỈ BÁO ---
    result_df = recent_df \
        .withColumn("bb_count", count("close").over(bb_w)) \
        .withColumn("bb_middle", avg("close").over(bb_w)) \
        .withColumn("bb_stddev", stddev("close").over(bb_w)) \
        .withColumn(
            "bb_upper",
            when(col("bb_count") < 20, None).otherwise(col("bb_middle") + 2 * col("bb_stddev"))
        ) \
        .withColumn(
            "bb_lower",
            when(col("bb_count") < 20, None).otherwise(col("bb_middle") - 2 * col("bb_stddev"))
        ) \
        .withColumn("bb_middle", when(col("bb_count") < 20, None).otherwise(col("bb_middle"))) \
        .withColumn(
            "bb_signal", # KHÔI PHỤC CỘT NÀY ĐỂ HIỂN THỊ GRAFANA
            when(col("bb_upper").isNull(), "NOT ENOUGH DATA")
            .when(col("close") > col("bb_upper"), "ABOVE UPPER BAND")
            .when(col("close") < col("bb_lower"), "BELOW LOWER BAND")
            .otherwise("INSIDE BAND")
        ) \
        .withColumn("sma_9_count", count("close").over(sma_short_w)) \
        .withColumn("sma_21_count", count("close").over(sma_long_w)) \
        .withColumn("sma_9", when(col("sma_9_count") < 9, None).otherwise(avg("close").over(sma_short_w))) \
        .withColumn("sma_21", when(col("sma_21_count") < 21, None).otherwise(avg("close").over(sma_long_w))) \
        .withColumn("sma_diff", col("sma_9") - col("sma_21")) \
        .withColumn("prev_sma_diff", lag("sma_diff").over(w)) \
        .withColumn(
            "sma_crossover",
            when(col("sma_9").isNull() | col("sma_21").isNull(), "NOT ENOUGH DATA")
            .when((col("prev_sma_diff") <= 0) & (col("sma_diff") > 0), "GOLDEN CROSS")
            .when((col("prev_sma_diff") >= 0) & (col("sma_diff") < 0), "DEATH CROSS")
            .when(col("sma_9") > col("sma_21"), "SMA9 ABOVE SMA21")
            .when(col("sma_9") < col("sma_21"), "SMA9 BELOW SMA21")
            .otherwise("NO CROSS")
        ) \
        .withColumn("prev_close", lag("close").over(w)) \
        .withColumn("price_diff", col("close") - col("prev_close")) \
        .withColumn("gain", when(col("price_diff") > 0, col("price_diff")).otherwise(0)) \
        .withColumn("loss", when(col("price_diff") < 0, abs(col("price_diff"))).otherwise(0)) \
        .withColumn("avg_gain_14", avg("gain").over(rsi_w)) \
        .withColumn("avg_loss_14", avg("loss").over(rsi_w)) \
        .withColumn("rsi_count", count("close").over(rsi_w)) \
        .withColumn(
            "rsi_14",
            when(col("rsi_count") < 14, None) # VÁ LỖI RSI_COUNT Ở ĐÂY
            .when(col("avg_loss_14") == 0, 100.0)
            .otherwise(100.0 - (100.0 / (1.0 + (col("avg_gain_14") / col("avg_loss_14")))))
        ) \
        .withColumn(
            "advice",
            when((col("rsi_14") <= 30) & (col("sma_crossover") == "GOLDEN CROSS"), "SUPER BUY (RSI<30 + Golden Cross)")
            .when((col("rsi_14") >= 70) & (col("sma_crossover") == "DEATH CROSS"), "SUPER SELL (RSI>70 + Death Cross)")
            .when(col("rsi_14") <= 30, "BUY SIGNAL - OVERSOLD (RSI < 30)")
            .when(col("rsi_14") >= 70, "SELL SIGNAL - OVERBOUGHT (RSI > 70)")
            .when(col("sma_crossover") == "GOLDEN CROSS", "BUY SIGNAL - GOLDEN CROSS")
            .when(col("sma_crossover") == "DEATH CROSS", "SELL SIGNAL - DEATH CROSS")
            .when(col("close") < col("bb_lower"), "WATCH BUY - BELOW LOWER BAND")
            .when(col("close") > col("bb_upper"), "WATCH SELL - ABOVE UPPER BAND")
            .otherwise("HOLD")
        )

    # 3. Chỉ chọn các trường cần thiết để ghi
    output_df = result_df.select(
        "symbol", "open", "high", "low", "close", "volume",
        "avg_price", "vwap", "price_change", "price_change_pct",
        "volatility", "trend", "candle_strength", "event_time",
        "window_start", "window_end",
        "bb_middle", "bb_upper", "bb_lower", "bb_signal",
        "sma_9", "sma_21", "sma_crossover",
        "rsi_14", "advice"
    )

    # 4. Ghi đè bảng Indicator để Grafana luôn đọc dữ liệu mới nhất
    output_df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.connection.uri", indicator_uri) \
        .save()

    print(f"Batch {batch_id} completed")

# 9. Khởi chạy luồng Streaming
query = ohlc_df.writeStream \
    .foreachBatch(process_batch) \
    .option(
        "checkpointLocation",
        "hdfs://my-hadoop-hadoop-hdfs-nn:9000/user/long/checkpoints/crypto-bb-sma-stream-v1/"
    ) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()