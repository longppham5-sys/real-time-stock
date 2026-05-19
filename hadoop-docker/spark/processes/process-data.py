from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count
from pyspark.sql.functions import col

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Student Performance Analysis") \
    .getOrCreate()

# 2. Đọc dữ liệu từ file students.csv
# Lưu ý: Bộ dữ liệu Student Performance gốc thường dùng dấu phân cách là ';'
df = spark.read.csv("students.csv", header=True, inferSchema=True, sep=",")
# Hiển thị cấu trúc dữ liệu
df.printSchema()

# --- CÂU HỎI 1 ---
# Lọc học sinh muốn học cao học (higher='yes') nhưng điểm G3 < 10
higher_low_g3 = df.filter((col("higher") == "yes") & (col("G3") < 10))
print("Danh sách học sinh muốn học cao học nhưng G3 < 10:")
higher_low_g3.show()

# --- CÂU HỎI 2 ---
# Đếm số học sinh học > 10 tiếng/tuần (Trong tập dữ liệu này, studytime = 4 tương ứng với > 10h)
study_hard_count = df.filter(col("studytime") == 4).count()
print(f"Số lượng học sinh học trên 10 tiếng/tuần: {study_hard_count}")

# --- CÂU HỎI 3 ---
# Tính điểm G3 trung bình theo nghề nghiệp của Mẹ (Mjob)
mjob_avg_g3 = df.groupBy("Mjob").agg(avg("G3").alias("Avg_G3")).orderBy(col("Avg_G3").desc())
mjob_avg_g3.show()
# So sánh teacher vs health: Kết quả phụ thuộc vào dữ liệu thực tế trong file của bạn.

# --- CÂU HỎI 4 ---
# So sánh điểm trung bình giữa nhóm có người yêu và độc thân
romantic_avg = df.groupBy("romantic").agg(avg("G3").alias("Avg_G3"))
romantic_avg.show()

# --- CÂU HỎI 5 ---
# Tạo cột Total_Alcohol = Dalc + Walc
df = df.withColumn("Total_Alcohol", col("Dalc") + col("Walc"))

# --- CÂU HỎI 6 ---
# Thêm cột Rank dựa vào điểm G3
df = df.withColumn("Rank", 
    when(col("G3") >= 15, "Gioi")
    .when(col("G3") >= 10, "Kha")
    .otherwise("Yeu")
)
df.select("G3", "Rank").show(10)

# --- CÂU HỎI 7 & 8 (SỬ DỤNG SQL) ---
# Đăng ký DataFrame thành một bảng tạm (Temporary View) để dùng SQL
df.createOrReplaceTempView("students_table")

# Câu 7: SQL lọc học sinh vắng > 10 buổi, G3 < 10 và hay đi chơi (goout > 4)
sql_query_7 = """
SELECT * FROM students_table 
WHERE absences > 10 AND G3 < 10 AND goout > 4
"""
print("Kết quả truy vấn SQL cho câu 7:")
spark.sql(sql_query_7).show()

# Câu 8: Query tính G3 trung bình và số lượng học sinh theo khu vực (address)
sql_query_8 = """
SELECT address, 
       AVG(G3) as Average_G3, 
       COUNT(*) as Student_Count 
FROM students_table 
GROUP BY address
"""
print("Kết quả thống kê theo khu vực (U: Thành thị, R: Nông thôn):")
spark.sql(sql_query_8).show()