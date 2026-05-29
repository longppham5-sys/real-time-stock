import websocket
import json
from kafka import KafkaProducer
import time
import os 

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'my-cluster-kafka-bootstrap.default.svc:9093')
TOPIC_NAME = 'crypto-prices'
CA_CERT_PATH = os.getenv('KAFKA_CA_CERT', '/etc/cluster-ca/ca.crt')
USER_CERT_PATH = os.getenv('KAFKA_USER_CERT', '/etc/producer-credentials/user.crt')
USER_KEY_PATH = os.getenv('KAFKA_USER_KEY', '/etc/producer-credentials/user.key')

print("Cấu hình mTLS giữa producer và Kafka...")
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
        refined_data = {
            "symbol": data['s'],
            "price": float(data['p']),
            "quantity": float(data['q']),
            "timestamp": data['E']
        }
        
        producer.send(TOPIC_NAME, value=refined_data).add_errback(lambda e: print(f"Gửi dữ liệu đến Kafka thất bại: {e}"))
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
    socket = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    
    ws = websocket.WebSocketApp(
        socket,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    ws.run_forever()