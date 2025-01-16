from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, from_json, col
from pyspark.sql.types import StructType, StringType, FloatType

# Tạo SparkSession với Kafka Connector
spark = SparkSession.builder \
    .appName("KafkaToSparkSQL") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit_stream") \
    .load()

# Dữ liệu Kafka chứa key và value ở dạng byte, chuyển đổi thành chuỗi
kafka_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Định nghĩa schema cho dữ liệu JSON
schema = StructType() \
    .add("title", StringType()) \
    .add("url", StringType()) \
    .add("score", FloatType()) \
    .add("created_utc", FloatType()) \
    .add("subreddit", StringType())

# Chuyển đổi dữ liệu JSON thành DataFrame
json_df = kafka_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

# Tạo bảng tạm để thực hiện các truy vấn SQL
json_df.createOrReplaceTempView("reddit_messages")

# Sử dụng Spark SQL để thực hiện truy vấn trên dữ liệu
result_df = spark.sql("""
    SELECT title, url, score, created_utc, subreddit
    FROM reddit_messages
    WHERE title IS NOT NULL
""")

# Ghi kết quả ra console
query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Chạy stream cho đến khi dừng lại
query.awaitTermination()

# Dừng SparkSession sau khi hoàn thành
spark.stop()
