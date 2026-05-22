from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, timestamp_seconds, window,
    max, min, when, abs,
    sum as spark_sum, avg,min_by, max_by
)
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
import os

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("CryptoProcessor") \
    .getOrCreate()

# Tối ưu partition
spark.sparkContext.setLogLevel("WARN")
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
    .withWatermark("event_time", "60 seconds") \
    .groupBy(
        window(col("event_time"), "60 seconds"),
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
    )

# 6. Tính toán các chỉ báo bổ sung
df_with_indicators = windowed_df \
    .withColumn("price_change",col("close") - col("open")) \
    .withColumn("price_change_pct",
        when(
            col("open") != 0,
            ((col("close") - col("open")) / col("open")) * 100
        ).otherwise(0)
    ) \
    .withColumn("volatility",
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
    .withColumn("body_size",abs(col("close") - col("open"))) \
    .withColumn("range_size",col("high") - col("low")) \
    .withColumn(
        "candle_strength",
        when(
            col("range_size") != 0,
            col("body_size") / col("range_size")
        ).otherwise(0)
    )

#transform to batch processing
def process_batch(batch_df, batch_id):
    # if batch is empty , skip
    if batch_df.isEmpty():
        return
        
    print(f"--- Processing Batch ID: {batch_id} ---")
    # 7. RSI Strategy
    from pyspark.sql.functions import lag,col,when,abs,round,avg,window
    from pyspark.sql.window import Window
    #RSI
    window_spec = Window.partitionBy("symbol").orderBy("event_time")    
    #Tinh gia chenh lech
    df_step1 = batch_df.withColumn("prev_close_price", lag("close",1).over(window_spec))
    df_step1 = df_step1.withColumn("change_price",round(col("close")-col("prev_close_price"),3))

    #Tach biet tang giam
    df_step2 = df_step1.withColumn("gain_close_price",\
                                    when(col("change_price")>=0,col("change_price")).otherwise(0))
    df_step2 = df_step2.withColumn("loss_close_price",\
                                    when(col("change_price")<0,abs("change_price")).otherwise(0))

    #Tính trung bình của cột Gain và cột Loss trong 14 phút gần nhất.
    window_14 = window_spec.rowsBetween(-13,Window.currentRow)
    df_step3 = df_step2.withColumn("avg_gain_14m",\
                                        round(avg("gain_close_price").over(window_14),3))
    df_step3 = df_step3.withColumn("avg_loss_14m",\
                                        round(avg("loss_close_price").over(window_14),3))  
        
    # Tinh RS 
    df_with_rsi = df_step3.withColumn("RSI",\
                                    when(col("avg_loss_14m") == 0,100)
                                    .otherwise(round(100-(100/(1+(col("avg_gain_14m") / col("avg_loss_14m")))),3)))                       

    # 8. Đánh giá mức độ rủi ro
    df_with_risk = df_with_rsi \
        .withColumn(
            "risk_level",
            when(col("volatility") >= 2, "HIGH")
            .when(col("volatility") >= 1, "MEDIUM")
            .otherwise("LOW")
        )

    #9. Thao: MACD, Volume Spike, Bollinger Bands (BB), On-Balance Volume (OBV)
    #Thao
    from pyspark.sql.functions import expr, col, avg, when, stddev, lag, sum 
    from pyspark.sql.window import Window

    # cau hinh cua so thoi gian (blueprint)
    window_12 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-11, 0)
    window_26 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-25, 0)
    window_signal = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-8, 0)
    window_vol = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-20, -1)

    # Cấu hình cửa sổ cho Bollinger Bands (Mặc định tính trên chu kỳ SMA 20 phiên)
    window_bb = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-19, 0)

    # Cấu hình cửa sổ lũy kế vô hạn cho OBV (Quét từ dòng đầu tiên đến dòng hiện tại)
    window_obv = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(Window.unboundedPreceding, 0)

    # Cấu hình cửa sổ lùi 1 dòng duy nhất để so sánh giá phiên hiện tại với phiên trước (Phục vụ OBV)
    window_lag1 = Window.partitionBy("symbol").orderBy("event_time")

    # TIẾN HÀNH TÍNH TOÁN CÁC CHỈ SỐ KỸ THUẬT
    # MACD
    df_indicators = df_with_risk \
        .withColumn("SMA_12", avg("close").over(window_12)) \
        .withColumn("SMA_26", avg("close").over(window_26))

    df_indicators = df_indicators.withColumn("MACD_Line", col("SMA_12") - col("SMA_26"))
    df_indicators = df_indicators.withColumn("Signal_Line", avg("MACD_Line").over(window_signal))
    df_indicators = df_indicators.withColumn("MACD_Histogram", round(col("MACD_Line") - col("Signal_Line"),3))
    df_indicators = df_indicators.drop("SMA_12", "SMA_26", "MACD_Line", "Signal_Line")

    # VOLUME SPIKE
    df_indicators = df_indicators.withColumn("Avg_Volume_Past", avg("volume").over(window_vol))

    df_indicators = df_indicators.withColumn(
        "Volume_Spike",
        when((col("volume") > (col("Avg_Volume_Past") * 1.5)) & (col("Avg_Volume_Past").isNotNull()), 1).otherwise(0)
    )
    df_indicators = df_indicators.drop("Avg_Volume_Past")

    # BOLLINGER BANDS (BB)
    df_indicators = df_indicators.withColumn("BB_Middle", avg("close").over(window_bb))
    df_indicators = df_indicators.withColumn("BB_StdDev", stddev("close").over(window_bb)) #Standard Deviation
    df_indicators = df_indicators.withColumn("BB_Upper", round(col("BB_Middle") + (col("BB_StdDev") * 2),3))
    df_indicators = df_indicators.withColumn("BB_Lower", round(col("BB_Middle") - (col("BB_StdDev") * 2),3))
    df_indicators = df_indicators.drop("BB_Middle","BB_StdDev")

    #ON-BALANCE VOLUME (OBV)
    df_indicators = df_indicators.withColumn("prev_close", lag("close", 1).over(window_lag1))
    df_indicators = df_indicators.withColumn("direction_volume",
        when(col("close") > col("prev_close"), col("volume"))
        .when(col("close") < col("prev_close"), -col("volume"))
        .otherwise(0.0)
    )
    df_final = df_indicators.withColumn("OBV", round(sum("direction_volume").over(window_obv),3))
    df_final = df_final.drop("prev_close","direction_volume")

    # 10. Trading Advice
    # conditions 
    # risk
    condition_extreme_risk = col("risk_level") == "HIGH"

    condition_bull_trap = (
        (col("price_change_pct") > 5) & 
        (col("close") > col("BB_Upper")) &
        (col("OBV") < 0) & 
        (col("Volume_Spike") == 0)
    )

    # BUY signals - high confidence - reversal from the bottom
    df_final = df_final.withColumn("OBV_prv", lag("OBV",1).over(window_lag1))

    condition_strong_buy = (
        (col("close") <= col("BB_Lower")) & 
        (col("RSI") < 30) & 
        (col("Volume_Spike") == 1) & 
        (col("OBV") > col("OBV_prv")) & 
        (col("MACD_Histogram") > 0) & 
        (col("candle_strength") > 0.7)
    )

    # BUY signals - normal confidence - the price witness a increase trend
    condition_buy_trend = (
        (col("trend") == "UP") &
        (col("close") > col("vwap")) &
        (col("close") <= col("BB_Upper")) &
        (col("OBV") > col("OBV_prv")) &
        (col("MACD_Histogram") > 0) &
        (col("RSI") >= 40) & (col("RSI") <= 65) &
        (col("risk_level") != "HIGH") &
        (col("volatility") < 2)
    )

    # SELL signals - high confidence - reversal from the top
    condition_strong_sell = (
        (col("close") >= col("BB_Upper")) & 
        (col("RSI") > 70) & 
        (col("Volume_Spike") == 1) & 
        (col("OBV") < col("OBV_prv")) & 
        (col("MACD_Histogram") < 0) & 
        (col("candle_strength") > 0.7)
    )

    # SELL signals - normal confidence - the price witness a decrease trend
    condition_sell_trend = (
        (col("trend") == "DOWN") &
        (col("close") < col("vwap")) &
        (col("OBV") < col("OBV_prv")) &
        (col("MACD_Histogram") < 0)
    )

    final_df = df_final.withColumn(
        "advice",
        when(condition_extreme_risk,"EXTREME RISK - AVOID TRADING")
        .when(condition_bull_trap,"POTENTIAL BULL TRAP - CAUTION ADVISED")
        .when(condition_strong_buy,"STRONG BUY SIGNAL - POTENTIAL REVERSAL FROM THE BOTTOM")
        .when(condition_strong_sell,"STRONG SELL SIGNAL - POTENTIAL REVERSAL FROM THE TOP")
        .when(condition_buy_trend,"BUY TREND - PRICE SHOWING UPWARD MOMENTUM")
        .when(condition_sell_trend,"SELL TREND - PRICE SHOWING DOWNWARD MOMENTUM")
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
        "RSI",
        "risk_level",
        "MACD_Histogram",
        "Volume_Spike",
        "BB_Upper",
        "BB_Lower",
        "OBV",
        "advice",
        "event_time"
    )
    # #test
    # final_df.show(truncate=False)
    final_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("spark.mongodb.connection.uri", mongo_uri) \
        .save()

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
query = df_with_indicators.writeStream \
    .foreachBatch(process_batch) \
    .option(
        "checkpointLocation",
        "hdfs://my-hadoop-hadoop-hdfs-nn:9000/user/long/checkpoints/crypto-processor"
    ) \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .start()

query.awaitTermination()

query.awaitTermination()