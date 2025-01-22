# app.py
import subprocess
from multiprocessing import Process
import time

def run_tiki_crawler():
    subprocess.call(["python", "tiki_crawler_producer.py"])

def run_spark_processor():
    subprocess.call(["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0", "spark_processor.py"])

if __name__ == "__main__":
    # Khởi chạy song song
    crawler_process = Process(target=run_tiki_crawler)
    spark_process = Process(target=run_spark_processor)
    
    crawler_process.start()
    spark_process.start()
    
    try:
        while True:
            time.sleep(1)  # Giữ chương trình chạy
    except KeyboardInterrupt:
        print("\n🛑 Stopping...")
        crawler_process.terminate()
        spark_process.terminate()