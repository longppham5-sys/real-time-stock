from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, desc

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("SalesAnalysis") \
    .getOrCreate()

# Giả sử file dữ liệu của bạn đã được đẩy lên HDFS
# Bạn có thể thay bằng đường dẫn file CSV thật của bạn
file_path = "hdfs://my-hadoop-hadoop-hdfs-nn:9000/sales_data.csv"

# 2. Đọc dữ liệu (Lazy)
# Lưu ý: header=True để lấy dòng đầu làm tên cột, inferSchema=True để tự đoán kiểu số/chữ
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 3. Tính tổng tiền cho mỗi dòng (Transformation - Lazy)
# Logic: Total = Quantity * UnitPrice
df_with_total = df.withColumn("Total_Money", round(col("Quantity") * col("UnitPrice"), 2))

# 4. Thống kê tổng doanh thu theo từng quốc gia (Transformation - Lazy)
# Logic: Map ra cặp (Country, Total_Money) và GroupBy
country_revenue = df_with_total.groupBy("Country") \
    .sum("Total_Money") \
    .withColumnRenamed("sum(Total_Money)", "Total_Revenue")

# 5. Sắp xếp để tìm quốc gia mua nhiều nhất sau UK (Transformation - Lazy)
# Chúng ta sẽ lọc bỏ United Kingdom trước, sau đó sắp xếp giảm dần
top_countries = country_revenue.filter(col("Country") != "United Kingdom") \
    .orderBy(desc("Total_Revenue"))

# 6. Thực thi và hiển thị kết quả (Action - Bắt đầu chạy thật)
print("--- Thống kê doanh thu theo từng quốc gia (Không tính UK) ---")
top_countries.show(10)

# Lấy ra quốc gia đứng đầu danh sách này
top_1_after_uk = top_countries.limit(1).collect()
if top_1_after_uk:
    print(f"Quốc gia mua hàng nhiều nhất sau UK là: {top_1_after_uk[0]['Country']}")

spark.stop()