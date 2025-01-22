# config.py
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",  # Sử dụng Kafka local
    "topic": "tiki_comments"                # Tên topic Kafka
}

SPARK_CONFIG = {
    "checkpoint_location": "/home/trungtran/IE212-Project/checkpoint",  # Đường dẫn local cho checkpoint
    "output_path": "/home/trungtran/IE212-Project/data"               # Đường dẫn lưu file CSV
}

MODEL_CONFIG = {
    "model_path": "models/bert_multilabel_model_trainer",  # Đường dẫn đến model
    "label_mapping": "models/label_mapping.pkl"            # Đường dẫn file label mapping
}