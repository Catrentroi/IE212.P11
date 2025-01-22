# spark_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, TimestampType
from bert_model import BERTModel
from config import SPARK_CONFIG, KAFKA_CONFIG

class SparkProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("TikiRealtimeBERT") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .getOrCreate()
        self.bert_model = BERTModel()
    
    def process_stream(self):
        # Schema của dữ liệu Kafka
        schema = StructType() \
            .add("content", StringType()) \
            .add("created_at", TimestampType()) \
            .add("product_id", StringType())
        
        # Đọc dữ liệu từ Kafka
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"]) \
            .option("subscribe", KAFKA_CONFIG["topic"]) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON và xử lý
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Dự đoán bằng BERT
        result_df = parsed_df.withColumn(
            "prediction",
            self.bert_model.predict(col("content"))
        )
        
        # Ghi kết quả
        query = result_df.writeStream \
            .format("csv") \
            .option("path", SPARK_CONFIG["output_path"]) \
            .option("checkpointLocation", SPARK_CONFIG["checkpoint_location"]) \
            .outputMode("append") \
            .start()
        
        return query

if __name__ == "__main__":
    processor = SparkProcessor()
    query = processor.process_stream()
    query.awaitTermination()