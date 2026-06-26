import os
import json

from datetime import datetime

from uuid6 import uuid7
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

# Tải biến môi trường
load_dotenv()
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")

# 1. Cấu hình Kafka Consumer (Người nhận tin)
conf = {
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'email_notification_group', # Tên nhóm dịch vụ
    'auto.offset.reset': 'earliest'         # Đọc từ những tin nhắn chưa được xử lý
}

consumer = Consumer(conf)

# 2. Đăng ký theo dõi Topic của Debezium
topic_name = 'pgstream_v3.public.orders'
consumer.subscribe([topic_name])

def process_and_send_email(msg_value):
    """Hàm bóc tách JSON và lưu file text"""
    try:
        # Chuyển chuỗi JSON thành Dictionary của Python
        data = json.loads(msg_value)
        payload = data.get('payload', {})
   
        # Chỉ kích hoạt gửi email khi có đơn hàng mới (op = 'c')
        if payload and payload.get('op') == 'c':
            after = payload.get('after', {})
            customer_name = after.get('customer_name')
            product_name = after.get('product_name')
            price = after.get('price')
            
            order_date = after.get('order_date') 
            ts_micro = after.get('order_date')
            if ts_micro:
                # Chia cho 1,000,000 để về giây rồi convert
                order_date = datetime.fromtimestamp(ts_micro / 1000000).strftime('%Y-%m-%d %H:%M:%S')
            # Khởi tạo thư mục
            folder_path = "data/email"
            os.makedirs(folder_path, exist_ok=True)
            
            # Format nội dung
            content = f"Khách hàng {customer_name} đã mua sản phẩm {product_name} với giá {price} vào lúc {order_date}"
            
            # Ghi ra file với UUID7
            file_path = os.path.join(folder_path, f"{uuid7()}.txt")
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(content)
                
            print(f"[📧 THÀNH CÔNG] Đã gửi thư cho {customer_name} -> {file_path}")
            
    except Exception as e:
        print(f"[❌ LỖI] Không thể xử lý tin nhắn: {e}")

# 3. Vòng lặp vô hạn để ứng dụng luôn chạy ngầm
print(f"🚀 Email Service đang chạy và lắng nghe tại topic '{topic_name}'...")
print("Nhấn Ctrl+C để thoát.\n" + "-"*50)

try:
    while True:
        # Liên tục hỏi Kafka xem có tin nhắn mới không (mỗi 1 giây)
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Đã đọc đến cuối danh sách tin nhắn hiện tại
                continue
            else:
                print(f"Lỗi Kafka: {msg.error()}")
                break
                
        # Nếu có tin nhắn, giải mã nó và ném vào hàm xử lý
        raw_json_string = msg.value().decode('utf-8')
        process_and_send_email(raw_json_string)

except KeyboardInterrupt:
    print("\n👋 Đã tắt dịch vụ Email.")
finally:
    # Luôn đóng kết nối an toàn khi tắt app
    consumer.close()