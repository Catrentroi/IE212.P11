# IE212 - BIGDATA

Xây dựng Hệ thống Phân tích Dựa trên khía cạnh Cảm xúc (ABSA) Trực tuyến sử dụng Spark và Kafka trên Dữ liệu Bình luận từ Tiki.

Trần Quốc Trung - 22521569
Nguyễn Công Nam Triều - 22521533
Dư Duy Tài - 22521272

GVDH: Đỗ Trọng Hợp

# Hướng dẫn cài đặt
## Bước 1: Tải mô hình
[Link tải mô hình](https://drive.google.com/drive/u/0/folders/1OaOV4WpV9vu21V5L946qtSO9T9skZvcI).
Tạo một thư mục model và lưu mô hình vào thư mục model
## Bước 2: Cài đặt thư viện
```
pip install -r requirement.txt
```
## Bước 3: Chạy crawl dữ liệu id product
```
python crawl_product.py
```
## Bước 4: Chạy spark 
```
python spark_processor.py
```
## Bước 5: Chạy streamlit
```
streamlit run realtime_dashboard.py
```

**Lưu ý**: Cần phải cài đặt Kafka và Spark trước khi chạy.
