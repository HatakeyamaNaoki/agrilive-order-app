from flask import Flask, request, jsonify
import requests
import json
import os
import base64
from datetime import datetime, timezone, timedelta

# LINE注文データ管理用のディレクトリ
LINE_ORDERS_DIR = "line_orders"
if not os.path.exists(LINE_ORDERS_DIR):
    os.makedirs(LINE_ORDERS_DIR)

def save_line_order_data(line_account, sender_name, image_data, message_text=""):
    """
    LINE注文データを保存
    """
    try:
        # 現在の日時を取得
        jst = timezone(timedelta(hours=9))
        current_time = datetime.now(jst)
        order_date = current_time.strftime("%Y/%m/%d")
        timestamp = current_time.strftime("%Y%m%d_%H%M%S")
        
        # 注文データを作成
        order_data = {
            "line_account": line_account,
            "sender_name": sender_name,
            "order_date": order_date,
            "timestamp": timestamp,
            "message_text": message_text,
            "image_filename": f"line_order_{timestamp}.png",
            "processed": False
        }
        
        # 画像データを保存
        image_path = os.path.join(LINE_ORDERS_DIR, order_data["image_filename"])
        with open(image_path, "wb") as f:
            f.write(image_data)
        
        # 注文データをJSONファイルに保存
        orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
        orders = []
        if os.path.exists(orders_file):
            with open(orders_file, "r", encoding="utf-8") as f:
                orders = json.load(f)
        
        orders.append(order_data)
        
        with open(orders_file, "w", encoding="utf-8") as f:
            json.dump(orders, f, ensure_ascii=False, indent=4)
        
        return True, "LINE注文データを保存しました。"
    except Exception as e:
        return False, f"LINE注文データ保存エラー: {e}"

# Flaskアプリを作成
app = Flask(__name__)

@app.route('/webhook/line', methods=['POST'])
def line_webhook():
    try:
        # LINEからのリクエストを処理
        data = request.get_json()
        print(f"LINE Webhook受信: {data}")
        
        # メッセージイベントを処理
        if data.get('events'):
            for event in data['events']:
                if event['type'] == 'message':
                    # 画像メッセージの場合
                    if event['message']['type'] == 'image':
                        # 送信者情報を取得
                        sender_id = event['source']['userId']
                        sender_name = "LINE送信者"  # 実際の実装ではLINE APIで名前を取得
                        
                        # 画像を取得
                        message_id = event['message']['id']
                        line_channel_access_token = os.getenv('LINE_CHANNEL_ACCESS_TOKEN')
                        
                        if line_channel_access_token:
                            # LINE APIから画像を取得
                            headers = {
                                'Authorization': f'Bearer {line_channel_access_token}'
                            }
                            response = requests.get(
                                f'https://api-data.line.me/v2/bot/message/{message_id}/content',
                                headers=headers
                            )
                            
                            if response.status_code == 200:
                                image_data = response.content
                                
                                # 注文データを保存
                                success, message = save_line_order_data(
                                    sender_id,  # LINEアカウントID
                                    sender_name,
                                    image_data,
                                    ""  # メッセージテキスト
                                )
                                
                                if success:
                                    print(f"LINE注文データを保存しました: {message}")
                                else:
                                    print(f"LINE注文データ保存エラー: {message}")
                            else:
                                print(f"LINE画像取得エラー: {response.status_code}")
                        else:
                            print("LINE_CHANNEL_ACCESS_TOKENが設定されていません")
        
        return jsonify({'status': 'ok'})
        
    except Exception as e:
        print(f"LINE Webhook処理エラー: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """ヘルスチェック用エンドポイント"""
    return jsonify({'status': 'healthy', 'service': 'line-webhook'})

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False) 