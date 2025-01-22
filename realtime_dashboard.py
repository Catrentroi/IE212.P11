# realtime_dashboard.py
import streamlit as st
import pandas as pd
import time
import plotly.express as px
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Cấu hình tự động reload file
class CSVHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith("predictions.csv"):
            st.experimental_rerun()

# Khởi tạo observer
observer = Observer()
observer.schedule(CSVHandler(), path="data/")
observer.start()

# Streamlit UI
st.title("📊 Realtime Comment Analysis Dashboard")

@st.cache_data
def load_data():
    try:
        return pd.read_csv("data/output/part-*.csv")  # Đọc tất cả file CSV mới nhất
    except:
        return pd.DataFrame()

# Hiển thị dữ liệu real-time
placeholder = st.empty()
while True:
    df = load_data()
    with placeholder.container():
        # Hiển thị 10 bản ghi mới nhất
        st.subheader("Latest Predictions")
        st.dataframe(df.tail(10))
        
        # Phân phối nhãn
        st.subheader("Prediction Distribution")
        if not df.empty:
            fig = px.pie(df, names='prediction', title='Distribution of Predictions')
            st.plotly_chart(fig)
    
    time.sleep(5)  # Cập nhật mỗi 5 giây