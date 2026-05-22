from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, abs, avg, expr, count
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("CryptoRSI14Processor") \
    .getOrCreate()

MONGO_USER = "databaseAdmin"
MONGO_PASS = "databaseAdmin123456"
MONGO_HOST = "mongodb-rs0.default.svc.cluster.local"
MONGO_DB = "crypto"

input_uri = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:27017/{MONGO_DB}.prices_downsampled"
    f"?authSource=admin"
    f"&tls=true"
)

output_uri = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:27017/{MONGO_DB}.prices_indicators_10s"
    f"?authSource=admin"
    f"&tls=true"
)

df = spark.read \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", input_uri) \
    .load()

df = df.filter(
    col("symbol").isNotNull() &
    col("close").isNotNull() &
    col("event_time").isNotNull()
)

# Chỉ xử lý dữ liệu gần đây để tránh đọc toàn bộ collection
df = df.filter(
    col("event_time") >= expr("current_timestamp() - INTERVAL 2 HOURS")
)
df = df.filter(col("symbol") == "BTCUSDT")

w = Window.partitionBy("symbol").orderBy("event_time")

rsi_w = Window.partitionBy("symbol") \
    .orderBy("event_time") \
    .rowsBetween(-13, 0)

result_df = df \
    .withColumn("prev_close", lag("close").over(w)) \
    .withColumn("change", col("close") - col("prev_close")) \
    .withColumn("gain", when(col("change") > 0, col("change")).otherwise(0)) \
    .withColumn("loss", when(col("change") < 0, abs(col("change"))).otherwise(0)) \
    .withColumn("period_count", count("close").over(rsi_w)) \
    .withColumn("avg_gain_14", avg("gain").over(rsi_w)) \
    .withColumn("avg_loss_14", avg("loss").over(rsi_w)) \
    .withColumn(
        "rsi_14",
        when(col("period_count") < 14, None)
        .when(col("avg_loss_14") == 0, 100)
        .otherwise(
            100 - (100 / (1 + (col("avg_gain_14") / col("avg_loss_14"))))
        )
    ) \
    .withColumn(
        "advice",
        when(col("rsi_14").isNull(), "NOT ENOUGH DATA")
        .when(col("rsi_14") <= 30, "BUY SIGNAL")
        .when(col("rsi_14") >= 70, "SELL SIGNAL")
        .otherwise("HOLD")
    )

output_df = result_df.select(
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "avg_price",
    "vwap",
    "event_time",
    "rsi_14",
    "advice"
)

output_df.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", output_uri) \
    .save()

spark.stop()