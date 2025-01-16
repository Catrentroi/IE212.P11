import praw
import time
from confluent_kafka import Producer
import json

# Cấu hình Reddit API
reddit = praw.Reddit(
    client_id='3PoP-STlWo9MVHqMZUOAQg',  # Thay bằng client_id của bạn
    client_secret='0A6DPU5Qt_qqPrUKJwvyuyCsmBgVNA',  # Thay bằng client_secret của bạn
    user_agent='ABSA Sentiment'   # Thay bằng user_agent của bạn
)

# Cấu hình Kafka Producer
producer = Producer({'bootstrap.servers': 'localhost:9092'})  # Địa chỉ Kafka broker

# Chọn subreddit để stream
subreddit = reddit.subreddit('all')

# Hàm stream dữ liệu từ Reddit và gửi vào Kafka
def stream_reddit_to_kafka():
    print("Đang stream dữ liệu từ Reddit vào Kafka...")
    for submission in subreddit.stream.submissions():
        # Chuẩn bị dữ liệu
        data = {
            'title': submission.title,
            'url': submission.url,
            'score': submission.score,
            'created_utc': submission.created_utc,
            'subreddit': submission.subreddit.display_name
        }
        
        # Chuyển dữ liệu thành JSON
        message = json.dumps(data)

        # Gửi dữ liệu vào Kafka
        producer.produce('reddit_stream', value=message)  # 'reddit_stream' là tên topic
        producer.flush()  # Đảm bảo dữ liệu được gửi đi

        print(f"Gửi bài: {submission.title} vào Kafka")
        time.sleep(5)  # Delay giữa các bài viết

if __name__ == '__main__':
    stream_reddit_to_kafka()
