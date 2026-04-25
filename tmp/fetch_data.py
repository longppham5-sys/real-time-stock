import websocket
import json
from kafka import KafkaProducer
import time
import os  # Thêm thư viện để đọc biến môi trường

# 1. Cấu hình Kafka (Lấy từ Environment Variables hoặc dùng default)
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'my-cluster-kafka-bootstrap.default.svc:9093')
TOPIC_NAME = 'crypto-prices'

# Đường dẫn đến các file Cert (Mount từ Secret vào Pod)
CA_CERT_PATH = os.getenv('KAFKA_CA_CERT', '/etc/cluster-ca/ca.crt')
USER_CERT_PATH = os.getenv('KAFKA_USER_CERT', '/etc/producer-credentials/user.crt')
USER_KEY_PATH = os.getenv('KAFKA_USER_KEY', '/etc/producer-credentials/user.key')

print("Configure mTLS between producer and kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        security_protocol='SSL',
        ssl_cafile=CA_CERT_PATH,   
        ssl_certfile=USER_CERT_PATH, 
        ssl_keyfile=USER_KEY_PATH,   
        acks=1,
        retries=5
    )
    print("Kết nối Kafka (SSL/mTLS) thành công!")
except Exception as e:
    print(f"Lỗi kết nối Kafka: {e}")
    exit(1)

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # Binance gửi dữ liệu trade thô
        # s: Symbol, p: Price, q: Quantity, E: Event Time
        refined_data = {
            "symbol": data['s'],
            "price": float(data['p']),
            "quantity": float(data['q']),
            "timestamp": data['E']
        }
        
        # Bắn dữ liệu vào Kafka
        # Thêm callback để kiểm tra lỗi gửi
        producer.send(TOPIC_NAME, value=refined_data).add_errback(lambda e: print(f"Kafka Send Error: {e}"))
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