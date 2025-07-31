import streamlit as st
import streamlit_authenticator as stauth
import json
import pandas as pd
import io
import datetime
import pytz
from config import get_openai_api_key, is_production, load_config, get_line_channel_access_token
from parser_infomart import parse_infomart
from parser_iporter import parse_iporter
from parser_mitsubishi import parse_mitsubishi
from parser_pdf import parse_pdf_handwritten
from prompt_line import get_line_order_prompt
from docx import Document
import pdfplumber
from PIL import Image
import base64
import os
from datetime import datetime, timezone, timedelta
import threading
from flask import Flask, request, jsonify
import requests

# LINE注文データ管理用のディレクトリ
LINE_ORDERS_DIR = "line_orders"
if not os.path.exists(LINE_ORDERS_DIR):
    os.makedirs(LINE_ORDERS_DIR)

# Flask Webhookアプリを作成
webhook_app = Flask(__name__)

@webhook_app.route('/webhook/line', methods=['POST'])
def line_webhook():
    """LINE公式アカウントからのWebhookを受信"""
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
                        line_channel_access_token = get_line_channel_access_token()
                        
                        if line_channel_access_token:
                            # LINE APIで送信者のプロフィール情報を取得
                            headers = {
                                'Authorization': f'Bearer {line_channel_access_token}'
                            }
                            
                            # 送信者名を取得
                            profile_response = requests.get(
                                f'https://api.line.me/v2/bot/profile/{sender_id}',
                                headers=headers
                            )
                            
                            if profile_response.status_code == 200:
                                profile_data = profile_response.json()
                                sender_name = profile_data.get('displayName', 'LINE送信者')
                                print(f"送信者情報: {sender_name} ({sender_id})")
                            else:
                                sender_name = "LINE送信者"
                                print(f"送信者情報取得エラー: {profile_response.status_code}")
                            
                            # 画像を取得
                            message_id = event['message']['id']
                            image_response = requests.get(
                                f'https://api-data.line.me/v2/bot/message/{message_id}/content',
                                headers=headers
                            )
                            
                            if image_response.status_code == 200:
                                image_data = image_response.content
                                print(f"画像取得成功: {len(image_data)} bytes")
                                
                                # 注文データを保存
                                success, message = save_line_order_data(
                                    sender_id,  # LINEアカウントID
                                    sender_name,
                                    image_data,
                                    ""  # メッセージテキスト
                                )
                                
                                if success:
                                    print(f"✅ LINE注文データを保存しました: {message}")
                                    # 成功レスポンスをLINEに返す
                                    return jsonify({'status': 'ok', 'message': '注文データを受信しました'})
                                else:
                                    print(f"❌ LINE注文データ保存エラー: {message}")
                                    return jsonify({'status': 'error', 'message': message}), 500
                            else:
                                print(f"❌ LINE画像取得エラー: {image_response.status_code}")
                                return jsonify({'status': 'error', 'message': '画像の取得に失敗しました'}), 500
                        else:
                            print("❌ LINE_CHANNEL_ACCESS_TOKENが設定されていません")
                            return jsonify({'status': 'error', 'message': '設定エラー'}), 500
        
        return jsonify({'status': 'ok'})
        
    except Exception as e:
        print(f"❌ LINE Webhook処理エラー: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@webhook_app.route('/health', methods=['GET'])
def health_check():
    """ヘルスチェック用エンドポイント"""
    return jsonify({'status': 'healthy', 'service': 'line-webhook'})

# Webhookサーバー起動を無効化（ポート競合を避けるため）
if is_production():
    print("🌐 Webhookサーバー起動を無効化しています（手動アップロード機能をご利用ください）")
    print(f"🌐 Webhook URL: https://agrilive-order-app.onrender.com/webhook/line")

def add_line_account(email, line_account):
    """
    LINEアカウント情報を追加・更新
    """
    try:
        dynamic_users = load_dynamic_users()
        
        if email in dynamic_users.get("users", {}):
            dynamic_users["users"][email]["line_account"] = line_account
        else:
            # 基本ユーザーにもLINEアカウント情報を追加
            base_credentials = load_credentials()
            if email in base_credentials["credentials"]["usernames"]:
                base_credentials["credentials"]["usernames"][email]["line_account"] = line_account
                # 基本認証情報を保存（ローカル環境の場合）
                if not is_production():
                    with open("credentials.json", "w", encoding="utf-8") as f:
                        json.dump(base_credentials, f, ensure_ascii=False, indent=4)
        
        # 動的ユーザー情報を保存
        if save_dynamic_users(dynamic_users):
            return True, "LINEアカウント情報を更新しました。"
        else:
            return False, "LINEアカウント情報の保存に失敗しました。"
    except Exception as e:
        return False, f"LINEアカウント情報更新エラー: {e}"

def get_line_account(email):
    """
    ユーザーのLINEアカウント情報を取得
    """
    try:
        # 動的ユーザーから確認
        dynamic_users = load_dynamic_users()
        if email in dynamic_users.get("users", {}):
            return dynamic_users["users"][email].get("line_account", "")
        
        # 基本ユーザーから確認
        base_credentials = load_credentials()
        if email in base_credentials["credentials"]["usernames"]:
            return base_credentials["credentials"]["usernames"][email].get("line_account", "")
        
        return ""
    except Exception as e:
        print(f"LINEアカウント取得エラー: {e}")
        return ""

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

def get_line_orders_for_user(email):
    """
    ユーザーに関連するLINE注文データを取得
    """
    try:
        orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
        if not os.path.exists(orders_file):
            print(f"❌ orders.jsonファイルが存在しません: {orders_file}")
            return []
        
        with open(orders_file, "r", encoding="utf-8") as f:
            all_orders = json.load(f)
        
        print(f"📊 全注文データ数: {len(all_orders)}")
        
        # ユーザーのLINE IDを取得
        user_line_id = get_line_account(email)
        print(f"👤 ユーザー: {email}, LINE ID: {user_line_id}")
        
        # LINE IDでフィルタ（公式アカウント経由の場合）
        if user_line_id:
            user_orders = [order for order in all_orders if order.get("line_account") == user_line_id]
            print(f"🔍 LINE IDでフィルタ: {len(user_orders)}件")
        else:
            # LINE IDが設定されていない場合は、ユーザー名で直接フィルタ（手動アップロード用）
            user_orders = [order for order in all_orders if order.get("line_account") == email]
            print(f"🔍 ユーザー名でフィルタ: {len(user_orders)}件")
        
        # デバッグ: 全注文データの詳細を表示
        for i, order in enumerate(all_orders):
            print(f"📋 注文{i+1}: line_account={order.get('line_account')}, sender_name={order.get('sender_name')}")
        
        return user_orders
    except Exception as e:
        print(f"LINE注文データ取得エラー: {e}")
        return []

def get_all_line_orders():
    """
    すべてのLINE注文データを取得（管理者用）
    """
    try:
        orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
        if not os.path.exists(orders_file):
            return []
        
        with open(orders_file, "r", encoding="utf-8") as f:
            all_orders = json.load(f)
        
        return all_orders
    except Exception as e:
        print(f"全LINE注文データ取得エラー: {e}")
        return []

def get_available_line_ids():
    """
    システムに登録されているLINE IDの一覧を取得
    """
    try:
        orders = get_all_line_orders()
        line_ids = set()
        
        for order in orders:
            if order.get("line_account"):
                line_ids.add(order["line_account"])
        
        return list(line_ids)
    except Exception as e:
        print(f"LINE ID一覧取得エラー: {e}")
        return []

def delete_processed_line_orders():
    """
    処理済みのLINE注文データを削除
    """
    try:
        orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
        if not os.path.exists(orders_file):
            return True, "削除対象のデータがありません"
        
        with open(orders_file, "r", encoding="utf-8") as f:
            all_orders = json.load(f)
        
        # 処理済みの注文を削除
        original_count = len(all_orders)
        remaining_orders = [order for order in all_orders if not order.get("processed", False)]
        deleted_count = original_count - len(remaining_orders)
        
        # 削除された注文の画像ファイルも削除
        deleted_orders = [order for order in all_orders if order.get("processed", False)]
        for order in deleted_orders:
            image_path = os.path.join(LINE_ORDERS_DIR, order['image_filename'])
            if os.path.exists(image_path):
                os.remove(image_path)
        
        # 残りの注文データを保存
        with open(orders_file, "w", encoding="utf-8") as f:
            json.dump(remaining_orders, f, ensure_ascii=False, indent=4)
        
        return True, f"{deleted_count}件の処理済みデータを削除しました"
    except Exception as e:
        return False, f"削除エラー: {e}"

def delete_line_order_by_timestamp(timestamp):
    """
    指定されたタイムスタンプのLINE注文データを削除
    """
    try:
        orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
        if not os.path.exists(orders_file):
            return False, "データファイルが見つかりません"
        
        with open(orders_file, "r", encoding="utf-8") as f:
            all_orders = json.load(f)
        
        # 指定されたタイムスタンプの注文を削除
        original_count = len(all_orders)
        remaining_orders = [order for order in all_orders if order['timestamp'] != timestamp]
        deleted_count = original_count - len(remaining_orders)
        
        if deleted_count == 0:
            return False, "指定されたデータが見つかりません"
        
        # 削除された注文の画像ファイルも削除
        deleted_orders = [order for order in all_orders if order['timestamp'] == timestamp]
        for order in deleted_orders:
            image_path = os.path.join(LINE_ORDERS_DIR, order['image_filename'])
            if os.path.exists(image_path):
                os.remove(image_path)
        
        # 残りの注文データを保存
        with open(orders_file, "w", encoding="utf-8") as f:
            json.dump(remaining_orders, f, ensure_ascii=False, indent=4)
        
        return True, f"データを削除しました"
    except Exception as e:
        return False, f"削除エラー: {e}"

def parse_line_order_with_openai(image_path, sender_name, message_text=""):
    """
    OpenAI APIを使用してLINE注文画像を解析
    """
    try:
        api_key = get_openai_api_key()
        if not api_key:
            raise Exception("OPENAI_API_KEYが設定されていません")
        
        import openai
        client = openai.OpenAI(api_key=api_key)
        
        # 画像をbase64エンコード
        with open(image_path, "rb") as image_file:
            image_data = base64.b64encode(image_file.read()).decode('utf-8')
        
        # システムプロンプト
        system_prompt = get_line_order_prompt()
        
        # OpenAI APIを呼び出し
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": f"送信者: {sender_name}\nメッセージ: {message_text}\n\nこのLINE注文を解析してください。"},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_data}"}}
                    ]
                }
            ],
            max_tokens=2000,
            temperature=0.1
        )
        
        # レスポンスを解析
        content = response.choices[0].message.content
        
        # JSONとして解析
        try:
            cleaned_content = content.strip()
            if cleaned_content.startswith('```json'):
                cleaned_content = cleaned_content[7:]
            if cleaned_content.endswith('```'):
                cleaned_content = cleaned_content[:-3]
            cleaned_content = cleaned_content.strip()
            
            parsed_data = json.loads(cleaned_content)
            
            # 発注日が空の場合は日本時間の本日を設定
            if not parsed_data.get("order_date"):
                jst = timezone(timedelta(hours=9))
                current_time = datetime.now(jst)
                parsed_data["order_date"] = current_time.strftime("%Y/%m/%d")
            
            return parsed_data
        except json.JSONDecodeError as e:
            raise Exception(f"JSON解析エラー: {e}")
            
    except Exception as e:
        raise Exception(f"LINE注文解析エラー: {e}")

def extract_pdf_images(pdf_bytes):
    """
    PDFからページ全体を画像として抽出してPIL Imageオブジェクトのリストを返す
    """
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            page_images = []
            for page_num, page in enumerate(pdf.pages):
                try:
                    # ページを画像としてレンダリング
                    page_image = page.to_image()
                    if page_image:
                        pil_image = page_image.original
                        page_images.append({
                            'page': page_num + 1,
                            'image': pil_image
                        })
                except Exception as e:
                    st.warning(f"ページ {page_num + 1} の画像化に失敗: {e}")
            
            return page_images
    except Exception as e:
        st.error(f"PDF画像抽出エラー: {e}")
        return []

def display_pdf_images(images, filename):
    """
    PDFから抽出したページ画像をWeb上に表示
    """
    if not images:
        st.info(f"{filename} から画像を抽出できませんでした。")
        return
    
    st.subheader(f"📄 {filename} の画像表示")
    
    # ページ画像を表示
    if len(images) == 1:
        img_data = images[0]
        st.write(f"**ページ {img_data['page']}**")
        st.image(img_data['image'], caption=f"ページ {img_data['page']}", width=400)
    else:
        # 複数ページの場合は2ページずつ1行で表示
        for i in range(0, len(images), 2):
            cols = st.columns(2)
            for j in range(2):
                if i + j < len(images):
                    img_data = images[i + j]
                    with cols[j]:
                        st.write(f"**ページ {img_data['page']}**")
                        st.image(img_data['image'], caption=f"ページ {img_data['page']}", width=400)

def is_admin(username):
    """
    管理者かどうかを判定
    """
    # 管理者メールアドレスのリスト
    admin_emails = [
        "n.hatakeyama@agrilive.co.jp"  # 実際の管理者メール
    ]
    return username in admin_emails

def get_all_users():
    """
    すべてのユーザー情報を取得（基本ユーザー + 動的ユーザー）
    """
    try:
        base_credentials = load_credentials()
        dynamic_users = load_dynamic_users()
        
        all_users = []
        
        # 基本ユーザー（Secret Files）
        for email, user_info in base_credentials['credentials']['usernames'].items():
            all_users.append({
                "email": email,
                "name": user_info.get("name", ""),
                "company": user_info.get("company", ""),
                "type": "基本ユーザー（Secret Files）",
                "created_date": "管理者設定"
            })
        
        # 動的ユーザー
        for email, user_info in dynamic_users.get("users", {}).items():
            all_users.append({
                "email": email,
                "name": user_info.get("name", ""),
                "company": user_info.get("company", ""),
                "type": "動的ユーザー",
                "created_date": "新規登録"
            })
        
        return all_users
    except Exception as e:
        print(f"ユーザー情報取得エラー: {e}")
        return []

def load_docx_html(filepath):
    doc = Document(filepath)
    html = ""
    for para in doc.paragraphs:
        # 空行も改行として反映
        html += f"{para.text}<br>"
    return html

def detect_csv_type(content_bytes):
    ENCODINGS = ["utf-8-sig", "utf-8", "cp932", "shift_jis"]
    debug_log = []
    for enc in ENCODINGS:
        try:
            file_str = content_bytes.decode(enc)
            sio = io.StringIO(file_str)
            first_line = sio.readline().strip().split(",")
            debug_log.append(f"[{enc}] first_line={first_line}")
            cell0 = first_line[0].replace('"', '').replace("'", '').strip() if first_line else ""
            if cell0 == "H":
                debug_log.append(f"判定: infomart ({enc})")
                return 'infomart', enc, debug_log
            elif cell0 == "伝票番号":
                debug_log.append(f"判定: iporter ({enc})")
                return 'iporter', enc, debug_log
        except Exception as e:
            debug_log.append(f"[{enc}] error: {e}")
    debug_log.append("判定: unknown")
    return 'unknown', None, debug_log

def add_user(email, name, company, password):
    import os
    
    # 動的ユーザー情報を読み込み
    dynamic_users = load_dynamic_users()
    
    # 基本認証情報も確認（重複チェック）
    base_credentials = load_credentials()
    all_users = merge_credentials(base_credentials, dynamic_users)
    
    if email in all_users['credentials']['usernames']:
        return False, "このメールアドレスは既に登録されています。"
    
    hashed_pw = stauth.Hasher.hash(password)
    
    # 動的ユーザー情報に追加
    dynamic_users["users"][email] = {
        "name": name,
        "company": company,
        "password": hashed_pw
    }
    
    # 動的ユーザーファイルに保存
    if save_dynamic_users(dynamic_users):
        return True, "アカウントを追加しました。"
    else:
        return False, "アカウントの保存に失敗しました。"

def load_dynamic_users():
    """
    動的に追加されたユーザー情報を読み込む
    """
    try:
        with open("dynamic_users.json", "r", encoding="utf-8") as f:
            dynamic_users = json.load(f)
            # デバッグ情報をログに出力
            print(f"動的ユーザー読み込み成功: {len(dynamic_users.get('users', {}))} ユーザー")
            return dynamic_users
    except FileNotFoundError:
        # ファイルが存在しない場合は空の構造を返す
        print("動的ユーザーファイルが見つかりません")
        return {"users": {}}
    except Exception as e:
        print(f"動的ユーザー読み込みエラー: {e}")
        return {"users": {}}

def save_dynamic_users(dynamic_users):
    """
    動的に追加されたユーザー情報を保存する
    """
    try:
        with open("dynamic_users.json", "w", encoding="utf-8") as f:
            json.dump(dynamic_users, f, ensure_ascii=False, indent=4)
        print(f"動的ユーザー保存成功: {len(dynamic_users.get('users', {}))} ユーザー")
        return True
    except Exception as e:
        print(f"動的ユーザー保存エラー: {e}")
        return False

def merge_credentials(base_credentials, dynamic_users):
    """
    基本認証情報と動的ユーザー情報を統合する
    """
    merged_credentials = base_credentials.copy()
    
    # 動的ユーザーを基本認証情報に追加
    for email, user_info in dynamic_users.get("users", {}).items():
        merged_credentials["credentials"]["usernames"][email] = {
            "email": email,
            "name": user_info.get("name", ""),
            "company": user_info.get("company", ""),
            "password": user_info.get("password", "")
        }
    
    return merged_credentials

# --- 認証 ---
def load_credentials():
    """
    認証情報を読み込む
    本番環境: Render Secrets Filesから
    ローカル環境: ファイルから
    """
    import os
    
    # 本番環境（Render）の場合
    if os.getenv('RENDER'):
        try:
            # Secret Filesから読み込みを試行
            secret_paths = [
                '/etc/secrets/credentials.json',
                'credentials.json',
                './credentials.json'
            ]
            
            for path in secret_paths:
                if os.path.exists(path):
                    with open(path, "r", encoding="utf-8") as f:
                        return json.load(f)
            
            # Secret Filesで見つからない場合、通常のファイルから読み込み
            with open("credentials.json", "r", encoding="utf-8") as f:
                return json.load(f)
                
        except Exception as e:
            print(f"認証情報読み込みエラー: {e}")
            # フォールバック: 通常のファイルから読み込み
            with open("credentials.json", "r", encoding="utf-8") as f:
                return json.load(f)
    
    # ローカル開発環境の場合
    with open("credentials.json", "r", encoding="utf-8") as f:
        return json.load(f)

# 基本認証情報と動的ユーザー情報を統合
base_credentials = load_credentials()
dynamic_users = load_dynamic_users()
credentials_config = merge_credentials(base_credentials, dynamic_users)

authenticator = stauth.Authenticate(
    credentials=credentials_config['credentials'],
    cookie_name=credentials_config['cookie']['name'],
    key=credentials_config['cookie']['key'],
    expiry_days=credentials_config['cookie']['expiry_days'],
    preauthorized=credentials_config['preauthorized']
)
st.set_page_config(page_title="受発注データ集計アプリ（アグリライブ）", layout="wide")

# 自動更新機能
if st.button("🔄 データを更新", key="refresh_data"):
    st.rerun()

st.image("会社ロゴ.png", width=220)
st.title("受発注データ集計アプリ（アグリライブ）")

# LINE公式アカウントWebhook情報表示
def show_webhook_info():
    """
    LINE公式アカウントWebhook情報を表示
    """
    import streamlit as st
    
    if is_production():
        # 現在のアプリのWebhook URL
        webhook_url = "https://agrilive-order-app.onrender.com/webhook/line"
        st.sidebar.markdown("---")
        st.sidebar.subheader("🔗 LINE Webhook URL")
        st.sidebar.code(webhook_url)
        st.sidebar.info("このURLをLINE公式アカウントのWebhook設定に設定してください")
        
        # ヘルスチェック
        try:
            import requests
            health_url = "https://agrilive-order-app.onrender.com/health"
            response = requests.get(health_url, timeout=5)
            if response.status_code == 200:
                st.sidebar.success("✅ Webhookサーバー稼働中")
            else:
                st.sidebar.warning("⚠️ Webhookサーバー応答なし")
        except:
            st.sidebar.warning("⚠️ Webhookサーバー接続エラー")
        
        # LINE設定情報
        try:
            line_token = get_line_channel_access_token()
            st.sidebar.success("✅ LINE_CHANNEL_ACCESS_TOKEN設定済み")
            # トークンの一部を表示（セキュリティのため）
            token_preview = line_token[:10] + "..." + line_token[-10:] if len(line_token) > 20 else "***"
            st.sidebar.info(f"トークン: {token_preview}")
        except Exception as e:
            st.sidebar.error(f"❌ LINE_CHANNEL_ACCESS_TOKEN未設定: {e}")
        
        # 最近の受信状況
        try:
            orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
            if os.path.exists(orders_file):
                with open(orders_file, "r", encoding="utf-8") as f:
                    all_orders = json.load(f)
                
                # 最近24時間の受信を確認
                from datetime import datetime, timedelta
                now = datetime.now()
                recent_orders = []
                
                for order in all_orders:
                    try:
                        order_time = datetime.strptime(order['timestamp'], "%Y%m%d_%H%M%S")
                        if now - order_time < timedelta(hours=24):
                            recent_orders.append(order)
                    except:
                        continue
                
                if recent_orders:
                    st.sidebar.success(f"✅ 過去24時間で {len(recent_orders)} 件受信")
                    for order in recent_orders[-3:]:  # 最新3件を表示
                        st.sidebar.info(f"📋 {order['sender_name']} - {order['order_date']}")
                else:
                    st.sidebar.info("📭 過去24時間の受信なし")
            else:
                st.sidebar.info("📭 まだ受信データなし")
        except Exception as e:
            st.sidebar.error(f"受信状況確認エラー: {e}")
        
        # LINE注文データの自動更新
        st.sidebar.markdown("---")
        st.sidebar.subheader("📱 LINE注文データ")
        
        # 最新のLINE注文データを表示
        line_orders = get_line_orders_for_user(username)
        if line_orders:
            st.sidebar.success(f"📱 LINE注文データ: {len(line_orders)}件")
            latest_orders = sorted(line_orders, key=lambda x: x['timestamp'], reverse=True)[:3]
            for order in latest_orders:
                with st.sidebar.expander(f"📋 {order['sender_name']} - {order['order_date']}"):
                    st.write(f"**送信者**: {order['sender_name']}")
                    st.write(f"**受信日**: {order['order_date']}")
                    if order.get('processed', False):
                        st.success("✅ 処理済み")
                    else:
                        st.warning("⏳ 未処理")
                    
                    # 削除ボタン
                    if st.sidebar.button(f"🗑️ 削除", key=f"sidebar_delete_{order['timestamp']}"):
                        success, message = delete_line_order_by_timestamp(order['timestamp'])
                        if success:
                            st.sidebar.success(message)
                            st.rerun()
                        else:
                            st.sidebar.error(message)
        else:
            st.sidebar.info("LINE注文データはありません")
            st.sidebar.info(f"ユーザー: {username}")

# --- サイドバー ---
if not st.session_state.get("authentication_status"):
    st.sidebar.markdown("---")
    st.sidebar.subheader("新規アカウント追加")
    new_email = st.sidebar.text_input("メールアドレス", key="new_email")
    new_name = st.sidebar.text_input("お名前", key="new_name")
    new_company = st.sidebar.text_input("会社名", key="new_company")
    new_password = st.sidebar.text_input("パスワード", type="password", key="new_pw")
    view_select = st.sidebar.radio(
        "ご確認ください",
        ("表示しない", "利用規約", "プライバシーポリシー"),
        index=0
    )
    agree_terms = st.sidebar.checkbox("利用規約・プライバシーポリシーに同意します", key="agree_terms")

    if st.sidebar.button("追加"):
        if not agree_terms:
            st.sidebar.warning("利用規約・プライバシーポリシーに同意が必要です。")
        elif new_email and new_name and new_company and new_password:
            ok, msg = add_user(new_email, new_name, new_company, new_password)
            if ok:
                st.sidebar.success(msg)
                # 成功時にフォームをクリア
                st.rerun()
            else:
                st.sidebar.error(msg)
        else:
            st.sidebar.warning("すべて入力してください。")

# --- ログインフォームを描画（必ずここで表示！） ---
authenticator.login(
    location='main',
    fields={
        "Form name": "Login",
        "Username": "Email",
        "Password": "Password",
        "Login": "Login"
    }
)

# --- ログイン画面の下に規約を表示（ここで順序調整） ---
if not st.session_state.get("authentication_status"):
    st.markdown("---")
    if 'view_select' not in locals():
        view_select = "表示しない"  # セッション直後の再実行対策
    if view_select == "利用規約":
        html = load_docx_html("利用規約.docx")
        st.markdown("### 利用規約")
        st.markdown(html, unsafe_allow_html=True)
    elif view_select == "プライバシーポリシー":
        html = load_docx_html("プライバシーポリシー.docx")
        st.markdown("### プライバシーポリシー")
        st.markdown(html, unsafe_allow_html=True)
    # 何も選択しなければ何も出さない

# --- ログイン後の画面 ---
if st.session_state.get("authentication_status"):
    username = st.session_state.get("username", "")
    name = st.session_state.get("name", "")
    config = load_config(user_id=username)
    
    # ログアウトボタンを一番上に配置
    authenticator.logout('ログアウト', 'sidebar')
    
    st.success(f"{name} さん、ようこそ！")
    
    # LINEアカウント設定
    st.sidebar.markdown("---")
    st.sidebar.subheader("📱 LINEアカウント設定")
    
    current_line_account = get_line_account(username)
    line_account = st.sidebar.text_input(
        "LINE ID",
        value=current_line_account,
        help="LINE公式アカウントに送信する際のLINE IDを設定してください（例: U1234567890abcdef）"
    )
    
    if st.sidebar.button("LINE IDを更新"):
        if line_account:
            success, message = add_line_account(username, line_account)
            if success:
                st.sidebar.success(message)
                st.rerun()
            else:
                st.sidebar.error(message)
        else:
            st.sidebar.warning("LINE IDを入力してください。")
    
    if current_line_account:
        st.sidebar.info(f"現在のLINE ID: {current_line_account}")
    else:
        st.sidebar.warning("LINE IDが設定されていません。")
    
    # LINE IDの確認方法を表示
    with st.sidebar.expander("📋 LINE IDの確認方法"):
        st.markdown("""
        **LINE IDの確認方法:**
        
        1. **LINE公式アカウントに画像を送信**
        2. **システムログでLINE IDを確認**
        3. **上記のLINE IDを設定**
        
        **例:** `U1234567890abcdef`
        """)
    
    # Webhook情報を表示（管理者のみ）
    if is_admin(username):
        show_webhook_info()
    
    # 管理者ダッシュボード
    try:
        if is_admin(username):
            st.sidebar.markdown("---")
            st.sidebar.subheader("管理者ダッシュボード")
            
            if st.sidebar.button("アカウント状況確認"):
                st.session_state.show_admin_dashboard = True
            
            if st.sidebar.button("通常画面に戻る"):
                st.session_state.show_admin_dashboard = False
        
        # 管理者ダッシュボードの表示
        if is_admin(username) and st.session_state.get("show_admin_dashboard", False):
            st.markdown("---")
            st.subheader("📊 管理者ダッシュボード")
            
            # 統計情報
            all_users = get_all_users()
            base_users = [u for u in all_users if u["type"] == "基本ユーザー（Secret Files）"]
            dynamic_users = [u for u in all_users if u["type"] == "動的ユーザー"]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("総ユーザー数", len(all_users))
            with col2:
                st.metric("基本ユーザー数", len(base_users))
            with col3:
                st.metric("動的ユーザー数", len(dynamic_users))
            
            # ユーザー一覧
            st.subheader("👥 ユーザー一覧")
            
            if all_users:
                # DataFrameに変換
                df_users = pd.DataFrame(all_users)
                df_users = df_users[["email", "name", "company", "type", "created_date"]]
                df_users.columns = ["メールアドレス", "お名前", "会社名", "ユーザータイプ", "作成日"]
                
                st.dataframe(
                    df_users,
                    use_container_width=True,
                    hide_index=True
                )
                
                # エクスポート機能
                csv = df_users.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="ユーザー一覧をCSVダウンロード",
                    data=csv,
                    file_name=f"ユーザー一覧_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("ユーザーが登録されていません。")
            
            # LINE注文データ情報
            st.subheader("📱 LINE注文データ情報")
            
            all_line_orders = get_all_line_orders()
            available_line_ids = get_available_line_ids()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("総LINE注文数", len(all_line_orders))
            with col2:
                st.metric("登録LINE ID数", len(available_line_ids))
            with col3:
                processed_orders = [order for order in all_line_orders if order.get("processed", False)]
                st.metric("処理済み注文数", len(processed_orders))
            
            # 処理済みデータ削除ボタン
            if processed_orders:
                if st.button("🗑️ 処理済みデータ一括削除", type="secondary"):
                    success, message = delete_processed_line_orders()
                    if success:
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)
            
            # LINE ID一覧
            if available_line_ids:
                st.subheader("📋 登録済みLINE ID一覧")
                for line_id in available_line_ids:
                    st.code(line_id)
            
            # システム情報
            st.subheader("⚙️ システム情報")
            col1, col2 = st.columns(2)
            
            with col1:
                st.info(f"**環境**: {'本番環境' if is_production() else '開発環境'}")
                st.info(f"**現在のユーザー**: {username}")
            
            with col2:
                st.info(f"**基本認証ファイル**: credentials.json")
                st.info(f"**動的ユーザーファイル**: dynamic_users.json")
            
            st.stop()  # 管理者ダッシュボード表示時は通常の機能をスキップ
    except Exception as e:
        st.error(f"管理者ダッシュボードエラー: {e}")
        # エラーが発生した場合は通常の機能を続行

    # デバッグ用: 動的ユーザー情報の確認（開発時のみ表示）
    if not is_production():
        st.sidebar.markdown("---")
        st.sidebar.subheader("デバッグ情報")
        try:
            dynamic_users = load_dynamic_users()
            st.sidebar.info(f"動的ユーザー数: {len(dynamic_users.get('users', {}))}")
            if dynamic_users.get('users'):
                st.sidebar.json(dynamic_users)
        except Exception as e:
            st.sidebar.error(f"動的ユーザー読み込みエラー: {e}")

    # OpenAI APIキー設定（開発環境のみ）
    if not is_production():
        st.sidebar.markdown("---")
        st.sidebar.subheader("OpenAI API設定（開発環境）")
        
        # 現在のAPIキーを取得
        try:
            current_key = get_openai_api_key()
            api_key = st.sidebar.text_input(
                "OpenAI APIキー",
                value=current_key,
                type="password",
                help="PDF解析に必要なOpenAI APIキーを入力してください"
            )
            
            if api_key and api_key != current_key:
                # 新しいAPIキーを設定
                os.environ['OPENAI_API_KEY'] = api_key
                st.sidebar.success("APIキーが更新されました")
            elif api_key:
                st.sidebar.success("APIキーが設定されています")
            else:
                st.sidebar.warning("PDF解析にはAPIキーが必要です")
        except Exception as e:
            st.sidebar.error(f"APIキーの取得に失敗: {e}")
            st.sidebar.info("ローカル開発時は.envファイルにOPENAI_API_KEYを設定してください")
    else:
        # 本番環境の場合 - APIキー情報は表示しない
        pass

    # LINE注文データの表示
    line_orders = get_line_orders_for_user(username)
    
    # LINE注文データの表示
    st.subheader("📱 LINE注文データ")
    
    # 統計情報
    if line_orders:
        total_orders = len(line_orders)
        unprocessed_orders = [order for order in line_orders if not order.get("processed", False)]
        processed_orders = [order for order in line_orders if order.get("processed", False)]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("総注文数", total_orders)
        with col2:
            st.metric("未処理", len(unprocessed_orders))
        with col3:
            st.metric("処理済み", len(processed_orders))
    
    # 手動アップロード機能
    with st.expander("📤 LINE画像を手動アップロード"):
        uploaded_line_image = st.file_uploader(
            "LINEの注文画像をアップロード",
            type=['png', 'jpg', 'jpeg'],
            key="line_image_upload"
        )
        
        if uploaded_line_image:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.image(uploaded_line_image, caption="アップロードされたLINE画像", width=400)
            
            with col2:
                sender_name = st.text_input("送信者名", value="", key="sender_name")
                message_text = st.text_area("メッセージ内容（オプション）", key="message_text")
                
                if st.button("LINE注文として保存", key="save_line_order"):
                    try:
                        # 画像データを保存
                        image_data = uploaded_line_image.read()
                        success, message = save_line_order_data(
                            username,  # ユーザー名をLINEアカウントIDとして使用
                            sender_name or "不明",
                            image_data,
                            message_text
                        )
                        
                        if success:
                            st.success("LINE注文データを保存しました！")
                            st.info(f"保存されたデータ: 送信者={sender_name or '不明'}, ユーザー={username}")
                            # 3秒間待機してからページを再読み込み
                            import time
                            time.sleep(3)
                            st.rerun()
                        else:
                            st.error(f"保存エラー: {message}")
                    except Exception as e:
                        st.error(f"保存エラー: {e}")
                        st.error(f"詳細: {str(e)}")
    
    # 既存のLINE注文データ表示
    if line_orders:
        # 未処理の注文のみを表示
        unprocessed_orders = [order for order in line_orders if not order.get("processed", False)]
        
        if unprocessed_orders:
            st.info(f"未処理のLINE注文が {len(unprocessed_orders)} 件あります。")
            
            for i, order in enumerate(unprocessed_orders):
                with st.expander(f"📋 {order['sender_name']} - {order['order_date']} ({order['timestamp']})"):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        # 画像を表示
                        image_path = os.path.join(LINE_ORDERS_DIR, order['image_filename'])
                        if os.path.exists(image_path):
                            st.image(image_path, caption=f"LINE注文画像", width=400)
                        
                        # 注文情報を表示
                        st.write(f"**送信者**: {order['sender_name']}")
                        st.write(f"**受信日**: {order['order_date']}")
                        if order.get('message_text'):
                            st.write(f"**メッセージ**: {order['message_text']}")
                    
                    with col2:
                        # 解析ボタン
                        if st.button(f"解析開始", key=f"parse_{order['timestamp']}"):
                            try:
                                with st.spinner("LINE注文を解析中..."):
                                    # OpenAI APIで解析
                                    parsed_data = parse_line_order_with_openai(
                                        image_path, 
                                        order['sender_name'], 
                                        order.get('message_text', '')
                                    )
                                    
                                    # 標準形式に変換
                                    records = []
                                    delivery_date = parsed_data.get("delivery_date", "")
                                    items = parsed_data.get("items", [])
                                    
                                    for item in items:
                                        record = {
                                            "order_id": item.get("order_id", ""),
                                            "order_date": parsed_data.get("order_date", order['order_date']),
                                            "delivery_date": delivery_date,
                                            "partner_name": parsed_data.get("partner_name", order['sender_name']),
                                            "product_code": item.get("product_code", ""),
                                            "product_name": item.get("product_name", ""),
                                            "quantity": item.get("quantity", ""),
                                            "unit": item.get("unit", ""),
                                            "unit_price": item.get("unit_price", ""),
                                            "amount": item.get("amount", ""),
                                            "remark": item.get("remark", ""),
                                            "data_source": f"LINE注文_{order['timestamp']}"
                                        }
                                        records.append(record)
                                    
                                    # 注文を処理済みにマーク
                                    orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
                                    with open(orders_file, "r", encoding="utf-8") as f:
                                        all_orders = json.load(f)
                                    
                                    for order_item in all_orders:
                                        if order_item['timestamp'] == order['timestamp']:
                                            order_item['processed'] = True
                                            break
                                    
                                    with open(orders_file, "w", encoding="utf-8") as f:
                                        json.dump(all_orders, f, ensure_ascii=False, indent=4)
                                    
                                    st.success("LINE注文の解析が完了しました！")
                                    st.rerun()
                                    
                            except Exception as e:
                                st.error(f"LINE注文解析エラー: {e}")
                        
                        # 削除ボタン
                        if st.button(f"削除", key=f"delete_{order['timestamp']}"):
                            # 注文データを削除
                            orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
                            with open(orders_file, "r", encoding="utf-8") as f:
                                all_orders = json.load(f)
                            
                            all_orders = [o for o in all_orders if o['timestamp'] != order['timestamp']]
                            
                            with open(orders_file, "w", encoding="utf-8") as f:
                                json.dump(all_orders, f, ensure_ascii=False, indent=4)
                            
                            # 画像ファイルも削除
                            if os.path.exists(image_path):
                                os.remove(image_path)
                            
                            st.success("LINE注文を削除しました。")
                            st.rerun()
        else:
            st.info("未処理のLINE注文はありません。")
    else:
        st.info("LINE注文データはありません。手動アップロード機能をご利用ください。")
    
    st.subheader("注文データファイルのアップロード")
    
    # PDF画像表示設定
    col1, col2 = st.columns([3, 1])
    with col1:
        uploaded_files = st.file_uploader(
            label="Infomart / IPORTER / PDF 等の注文データファイルをここにドラッグ＆ドロップまたは選択してください",
            accept_multiple_files=True,
            type=['txt', 'csv', 'xlsx', 'pdf']
        )
    with col2:
        show_pdf_images = st.checkbox("PDF画像を表示", value=True, help="PDFファイルの画像を表示するかどうかを設定します")

    records = []
    debug_details = []
    
    # LINE注文データをrecordsに追加
    line_orders = get_line_orders_for_user(username)
    processed_line_orders = [order for order in line_orders if order.get("processed", False)]
    
    for order in processed_line_orders:
        # 処理済みのLINE注文データをrecordsに追加
        image_path = os.path.join(LINE_ORDERS_DIR, order['image_filename'])
        if os.path.exists(image_path):
            try:
                # OpenAI APIで解析済みデータを取得
                parsed_data = parse_line_order_with_openai(
                    image_path, 
                    order['sender_name'], 
                    order.get('message_text', '')
                )
                
                delivery_date = parsed_data.get("delivery_date", "")
                items = parsed_data.get("items", [])
                
                for item in items:
                    record = {
                        "order_id": item.get("order_id", ""),
                        "order_date": parsed_data.get("order_date", order['order_date']),
                        "delivery_date": delivery_date,
                        "partner_name": parsed_data.get("partner_name", order['sender_name']),
                        "product_code": item.get("product_code", ""),
                        "product_name": item.get("product_name", ""),
                        "quantity": item.get("quantity", ""),
                        "unit": item.get("unit", ""),
                        "unit_price": item.get("unit_price", ""),
                        "amount": item.get("amount", ""),
                        "remark": item.get("remark", ""),
                        "data_source": f"LINE注文_{order['timestamp']}"
                    }
                    records.append(record)
            except Exception as e:
                st.warning(f"LINE注文データの読み込みに失敗: {e}")
    if uploaded_files:
        for file in uploaded_files:
            filename = file.name
            content = file.read()

            if filename.lower().endswith((".txt", ".csv")):
                filetype, detected_enc, debug_log = detect_csv_type(content)
                debug_details.append(f"【{filename}】\n" + "\n".join(debug_log))
                file_like = io.BytesIO(content)
                if filetype == 'infomart':
                    records += parse_infomart(file_like, filename)
                elif filetype == 'iporter':
                    records += parse_iporter(file_like, filename)
                else:
                    st.warning(f"{filename} は未対応のフォーマットです")

            elif filename.lower().endswith(".xlsx"):
                try:
                    df_excel = pd.read_excel(io.BytesIO(content), sheet_name=0, header=None)
                    if df_excel.shape[0] > 5 and str(df_excel.iloc[4, 1]).strip() == "伝票番号":
                        file_like = io.BytesIO(content)
                        records += parse_mitsubishi(file_like, filename)
                    else:
                        st.warning(f"{filename} は未対応のExcelフォーマットです")
                except Exception as e:
                    st.error(f"{filename} の読み込みに失敗しました: {e}")
            
            elif filename.lower().endswith(".pdf"):
                # PDF画像の抽出と表示
                if show_pdf_images:
                    pdf_images = extract_pdf_images(content)
                    if pdf_images:
                        display_pdf_images(pdf_images, filename)
                
                # PDF解析の実行
                try:
                    with st.spinner(f"{filename} を解析中..."):
                        # APIキーの事前確認
                        try:
                            from config import get_openai_api_key
                            api_key = get_openai_api_key()
                            if not api_key:
                                st.error("OpenAI APIキーが設定されていません")
                                continue
                        except Exception as api_error:
                            st.error(f"APIキー取得エラー: {api_error}")
                            continue
                        
                        pdf_records = parse_pdf_handwritten(content, filename)
                        records += pdf_records
                        # 商品情報の抽出状況を確認
                        if pdf_records and pdf_records[0].get('product_name') == "商品情報なし":
                            st.warning("商品情報の抽出に失敗しました。手書き文字の認識精度を確認してください。")
                    st.success(f"{filename} の解析が完了しました")
                except Exception as e:
                    st.error(f"{filename} の解析に失敗しました: {e}")
                    st.error(f"詳細エラー: {str(e)}")
                    # 本番環境での追加情報
                    if is_production():
                        st.info("本番環境でのトラブルシューティング:")
                        st.info("1. Render Secrets FilesでOPENAI_API_KEYが正しく設定されているか確認")
                        st.info("2. アプリケーションを再デプロイして環境変数を反映")
                        st.info("3. Renderのログで詳細なエラー情報を確認")
    
    # レコードが存在する場合（空でも表示）
    if records:        
        df = pd.DataFrame(records)
        
        # 空行除外の条件を緩和（商品名または備考に値がある場合は表示）
        if not df.empty:
            # 商品名または備考に値がある行のみを保持
            df = df[df['product_name'].notna() | df['remark'].notna()]
        
        if not df.empty:
            columns = [
                "order_id", "order_date", "delivery_date", "partner_name",
                "product_code", "product_name", "quantity", "unit", "unit_price", "amount", "remark", "data_source"
            ]
            df = df.reindex(columns=columns)
            df.columns = ["伝票番号", "発注日", "納品日", "取引先名", "商品コード", "商品名", "数量", "単位", "単価", "金額", "備考", "データ元"]

            edited_df = st.data_editor(
                df,
                use_container_width=True,
                num_rows="dynamic",
                key="editor",
                hide_index=True
            )
        else:
            st.warning("表示可能なデータがありません。商品情報の抽出に失敗した可能性があります。")

        for col in ["発注日", "納品日"]:
            edited_df[col] = pd.to_datetime(edited_df[col], errors="coerce").dt.strftime("%Y/%m/%d")

        edited_df["数量"] = pd.to_numeric(edited_df["数量"], errors="coerce").fillna(0)

        df_sorted = edited_df.sort_values(
            by=["商品名", "納品日", "発注日"], na_position="last"
        )

        df_agg = (
            df_sorted
            .groupby(["商品名", "備考", "単位"], dropna=False, as_index=False)
            .agg({"数量": "sum"})
        )
        df_agg = df_agg[["商品名", "備考", "数量", "単位"]]
        df_agg = df_agg.sort_values(by=["商品名"])
        output = io.BytesIO()
        jst = pytz.timezone("Asia/Tokyo")
        now_str = datetime.now(jst).strftime("%y%m%d_%H%M")

        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            header_format = workbook.add_format({'bold': False, 'border': 0})

            # ▼ 注文一覧シート
            sheet1 = "注文一覧"
            edited_df.to_excel(writer, index=False, sheet_name=sheet1, startrow=1, header=False)
            worksheet1 = writer.sheets[sheet1]
            for col_num, value in enumerate(edited_df.columns.values):
                worksheet1.write(0, col_num, value, header_format)

            # ▼ 注文一覧(層別結果)シート
            sheet2 = "注文一覧(層別結果)"
            df_sorted.to_excel(writer, index=False, sheet_name=sheet2, startrow=1, header=False)
            worksheet2 = writer.sheets[sheet2]
            for col_num, value in enumerate(df_sorted.columns.values):
                worksheet2.write(0, col_num, value, header_format)

            # ▼ 集計結果シート
            sheet3 = "集計結果"
            df_agg.to_excel(writer, index=False, sheet_name=sheet3, startrow=1, header=False)
            worksheet3 = writer.sheets[sheet3]
            for col_num, value in enumerate(df_agg.columns.values):
                worksheet3.write(0, col_num, value, header_format)

        output.seek(0)
        
        # ダウンロードボタンと削除ボタンを横に並べる
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.download_button(
                label="Excelをダウンロード",
                data=output,
                file_name=f"{now_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col2:
            if processed_line_orders:  # 処理済みデータがある場合のみ削除ボタンを表示
                if st.button("🗑️ 処理済みデータ削除", type="secondary"):
                    success, message = delete_processed_line_orders()
                    if success:
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)
    else:
        st.info("注文ファイルをアップロードしてください")

elif st.session_state.get("authentication_status") is False:
    st.error("ユーザー名またはパスワードが正しくありません。")
elif st.session_state.get("authentication_status") is None:
    st.warning("ログイン情報を入力してください。")
