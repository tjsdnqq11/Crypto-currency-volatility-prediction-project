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
bucket = 'eth_ticker'  # 버킷 이름

# InfluxDB 클라이언트 초기화
client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

def on_message(ws, message):
    # Decode the incoming message
    data = json.loads(message.decode('utf-8'))

    # InfluxDB data format
    point = Point("eth_ticker").tag("code", data["code"]).tag("type", data["type"]).tag("ask_bid", data["ask_bid"]).tag("market_warning", data["market_warning"]).tag("stream_type", data["stream_type"]).time(data["timestamp"], WritePrecision.MS)

    # Add fields
    point.field("opening_price", data["opening_price"]).field("high_price", data["high_price"]).field("low_price", data["low_price"]).field("trade_price", data["trade_price"]).field("prev_closing_price", data["prev_closing_price"]).field("acc_trade_price", data["acc_trade_price"]).field("change", data["change"]).field("change_price", data["change_price"]).field("signed_change_price", data["signed_change_price"]).field("change_rate", float(data["change_rate"])).field("signed_change_rate", data["signed_change_rate"]).field("trade_volume", data["trade_volume"]).field("acc_trade_volume", data["acc_trade_volume"]).field("trade_date", data["trade_date"]).field("trade_time", data["trade_time"]).field("trade_timestamp", data["trade_timestamp"]).field("acc_ask_volume", data["acc_ask_volume"]).field("acc_bid_volume", data["acc_bid_volume"]).field("highest_52_week_price", data["highest_52_week_price"]).field("highest_52_week_date", data["highest_52_week_date"]).field("lowest_52_week_price", data["lowest_52_week_price"]).field("lowest_52_week_date", data["lowest_52_week_date"]).field("is_trading_suspended", data["is_trading_suspended"]).field("acc_trade_price_24h", data["acc_trade_price_24h"]).field("acc_trade_volume_24h", data["acc_trade_volume_24h"])

    # 데이터 쓰기
    write_api.write(bucket, org, point)

def on_connect(ws):
    print("connected!")
    # Request after connection
    # 구독 데이터 조립
    subscribe_fmt = [
    {
        "ticket": "test example"
    },
    {
        "type": "ticker",
        "codes": [
        "KRW-ETH",
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
    # 클라이언트 종료
    client.close()
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
