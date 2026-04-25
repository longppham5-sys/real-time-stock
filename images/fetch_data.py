import websocket
import json
from kafka import KafkaProducer
import time

# 1. Cấu hình Kafka
# Lưu ý: Thay đổi địa chỉ này nếu script chạy ngoài cụm K8s
KAFKA_BOOTSTRAP_SERVERS = 'my-cluster-kafka-bootstrap.default.svc:9092' 
TOPIC_NAME = 'crypto-prices'

print("Đang khởi tạo kết nối tới Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Tăng tính an toàn khi gửi dữ liệu
        acks=1,
        retries=5
    )
    print("Kết nối Kafka thành công!")
except Exception as e:
    print(f"Lỗi kết nối Kafka: {e}")
    exit(1)

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # Binance gửi dữ liệu trade thô, mình lọc lại các trường cần thiết
        # p: Price (Giá), q: Quantity (Số lượng), E: Event Time (Thời gian)
        refined_data = {
            "symbol": data['s'],
            "price": float(data['p']),
            "quantity": float(data['q']),
            "timestamp": data['E']
        }
        
        # Bắn dữ liệu vào Kafka
        producer.send(TOPIC_NAME, value=refined_data)
        print(f"Sent: {refined_data['symbol']} - {refined_data['price']}")
        
    except Exception as e:
        print(f"Lỗi xử lý tin nhắn: {e}")

def on_error(ws, error):
    print(f"Lỗi WebSocket: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### WebSocket đã đóng ###")

def on_open(ws):
    print("Đã mở kết nối tới Binance WebSocket!")

if __name__ == "__main__":
    # URL Stream giá BTC/USDT của Binance
    socket = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    
    ws = websocket.WebSocketApp(
        socket,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Chạy liên tục
    ws.run_forever()