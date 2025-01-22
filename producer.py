# spark_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf
from pyspark.sql.types import StructType, StringType
import pandas as pd
import torch
from transformers import BertTokenizerFast, BertForSequenceClassification
from config import SPARK_CONFIG, KAFKA_CONFIG, MODEL_CONFIG

# Load model BERT
model = BertForSequenceClassification.from_pretrained(MODEL_CONFIG["model_path"])
tokenizer = BertTokenizerFast.from_pretrained(MODEL_CONFIG["model_path"])
model.eval()

# Định nghĩa UDF cho Spark
@pandas_udf(StringType())
def predict_bert(texts: pd.Series) -> pd.Series:
    inputs = tokenizer(
        texts.tolist(),
        padding=True,
        truncation=True,
        max_length=512,
        return_tensors="pt"
    )
    with torch.no_grad():
        outputs = model(**inputs)
    preds = torch.argmax(outputs.logits, dim=1).numpy()
    return pd.Series([str(p) for p in preds])

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("TikiRealtimeBERT") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Schema dữ liệu từ Kafka
schema = StructType() \
    .add("content", StringType()) \
    .add("product_id", StringType())

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"]) \
    .option("subscribe", KAFKA_CONFIG["topic"]) \
    .load()

# Xử lý dữ liệu
processed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
.withColumn("prediction", predict_bert(col("content")))

# Ghi kết quả
query = processed_df.writeStream \
    .format("csv") \
    .option("path", SPARK_CONFIG["output_path"]) \
    .option("checkpointLocation", SPARK_CONFIG["checkpoint_location"]) \
    .outputMode("append") \
    .start()

query.awaitTermination()