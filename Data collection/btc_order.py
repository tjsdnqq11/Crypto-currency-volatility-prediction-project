from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import jwt  # PyJWT
import uuid
import websocket  # websocket-client
import json
import time

# InfluxDB 접속 정보
url = 'http://localhost:8086'  # InfluxDB 서버 주소
token = '9XmVwizKEHy7DY6gTRX1GJU4aCSdnzYaywPri_oR46EMGzks1fAzcOyuegB-LowygahuvJOLaKmFLYOdaAlBWw=='
org = 'tmer'  # 조직 이름
bucket = 'btc_order'  # 버킷 이름

# InfluxDB 클라이언트 초기화
client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

def on_message(ws, message):
    # Decode the incoming message
    data = json.loads(message.decode('utf-8'))
    
    # Extract order book units
    orderbook_units = data["orderbook_units"]

    # Create fields for each order book unit
    fields = {
        "total_ask_size": data["total_ask_size"],
        "total_bid_size": data["total_bid_size"]
    }

    for i, unit in enumerate(orderbook_units, start=1):
        fields[f"ask_price_{i}"] = unit["ask_price"]
        fields[f"bid_price_{i}"] = unit["bid_price"]
        fields[f"ask_size_{i}"] = float(unit["ask_size"])
        fields[f"bid_size_{i}"] = float(unit["bid_size"])

    # InfluxDB data format
    point = Point("btc_order").tag("code", data["code"]).tag("type", data["type"]).tag("stream_type", data["stream_type"]).time(data["timestamp"], WritePrecision.MS)

    for field, value in fields.items():
        point.field(field, value)

    # 데이터 쓰기
    write_api.write(bucket, org, point)

def on_connect(ws):
    print("connected!")
    # Request after connection
    # 구독 데이터 조립
    subscribe_fmt = [
    {
        "ticket": "test"
    },
    {
        "type": "orderbook",
        "codes": [
        "KRW-BTC.15"
        ]
    },
    {
        "format": "DEFAULT"
    }
    ]

    subscribe_data = json.dumps(subscribe_fmt)

    ws.send(subscribe_data)

def on_error(ws, err):
    print(err)

def on_close(ws, status_code, msg):
    print("closed!")


# WebSocket 연결 및 재연결을 위한 함수
def run_websocket():
    payload = {
        'access_key': "3xHQLYdx2cKystlvylU5l1smsNxqxoKZ0qxoYyRt",
        'nonce': str(uuid.uuid4()),
    }

    jwt_token = jwt.encode(payload, "fLQGNiiOzxm6uN4UTJn0uGj0pE7mV8hwnATfdFHk")
    authorization_token = f'Bearer {jwt_token}'
    headers = {"Authorization": authorization_token}

    ws_app = websocket.WebSocketApp("wss://api.upbit.com/websocket/v1",
                                    header=headers,
                                    on_message=on_message,
                                    on_open=on_connect,
                                    on_error=on_error,
                                    on_close=on_close)
    ws_app.run_forever()

# 무한 루프를 통한 재연결 시도
while True:
    try:
        run_websocket()
    except Exception as e:
        print("Error occurred:", e)
    # 연결이 끊어진 후 재연결 전에 잠시 대기
    time.sleep(1)
