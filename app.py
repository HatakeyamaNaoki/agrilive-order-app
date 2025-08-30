import streamlit as st
import streamlit_authenticator as stauth
import json
import pandas as pd
import io
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
import requests
import sqlite3

# LINE注文データ管理用のディレクトリ
LINE_ORDERS_DIR = "line_orders"
if not os.path.exists(LINE_ORDERS_DIR):
    os.makedirs(LINE_ORDERS_DIR, exist_ok=True)

def get_file_lock(file_path, timeout=10):
    """
    ファイルロックを取得する（Render環境では無効化）
    """
    import os
    
    # Render環境ではファイルロックを無効化（Read-only file system対策）
    if os.getenv('RENDER'):
        class DummyLock:
            def __enter__(self): return self
            def __exit__(self, *args): pass
        return DummyLock()
    
    try:
        import filelock
        lock_file = f"{file_path}.lock"
        return filelock.FileLock(lock_file, timeout=timeout)
    except ImportError:
        # filelockが利用できない場合はダミーロック
        class DummyLock:
            def __enter__(self): return self
            def __exit__(self, *args): pass
        return DummyLock()

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
            "processed": False,
            "parsed_data": None  # 解析結果を保存するフィールドを追加
        }
        
        # 画像データを保存
        image_path = os.path.join(LINE_ORDERS_DIR, order_data["image_filename"])
        with open(image_path, "wb") as f:
            f.write(image_data)
        
        # 注文データをJSONファイルに保存（ファイルロック付き）
        orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
        with get_file_lock(orders_file):
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

def save_parsed_line_order_data(timestamp, parsed_data):
    """
    LINE注文の解析結果を保存
    """
    try:
        orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
        if not os.path.exists(orders_file):
            return False, "注文データファイルが見つかりません"
        
        with get_file_lock(orders_file):
            with open(orders_file, "r", encoding="utf-8") as f:
                orders = json.load(f)
            
            # 指定されたタイムスタンプの注文を更新
            for order in orders:
                if order['timestamp'] == timestamp:
                    order['parsed_data'] = parsed_data
                    order['processed'] = True
                    break
            
            with open(orders_file, "w", encoding="utf-8") as f:
                json.dump(orders, f, ensure_ascii=False, indent=4)
        
        return True, "解析結果を保存しました"
    except Exception as e:
        return False, f"解析結果保存エラー: {e}"

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
        print(f"👤 ユーザー: {email}")
        
        # ユーザー名で直接フィルタ（手動アップロード用）
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



def delete_processed_line_orders():
    """
    処理済みのLINE注文データを削除
    """
    try:
        orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
        if not os.path.exists(orders_file):
            return True, "削除対象のデータがありません"
        
        with get_file_lock(orders_file):
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
        
        with get_file_lock(orders_file):
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

def parse_line_order_with_openai(image_path, sender_name, message_text="", order_date=""):
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
        
        # 受信日時を含むユーザーメッセージ
        user_message = f"送信者: {sender_name}\n受信日: {order_date}\nメッセージ: {message_text}\n\nこのLINE注文を解析してください。受信日を基準に納品日を計算してください。"
        
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
                        {"type": "text", "text": user_message},
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
            
            # 発注日が空の場合は受信日を設定
            if not parsed_data.get("order_date"):
                parsed_data["order_date"] = order_date
            
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

def validate_email(email):
    """
    メールアドレスの形式を検証する
    """
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False, "有効なメールアドレスを入力してください"
    return True, "メールアドレスは有効です"

def validate_password(password):
    """
    パスワードの強度を検証する
    """
    if len(password) < 8:
        return False, "パスワードは8文字以上である必要があります"
    
    if not any(c.isupper() for c in password):
        return False, "パスワードには大文字が含まれる必要があります"
    
    if not any(c.islower() for c in password):
        return False, "パスワードには小文字が含まれる必要があります"
    
    if not any(c.isdigit() for c in password):
        return False, "パスワードには数字が含まれる必要があります"
    
    return True, "パスワードは有効です"

def add_user(email, name, company, password):
    """
    動的にユーザーを追加する（SQLiteデータベース使用）
    """
    import os
    
    print(f"add_user開始: email={email}, name={name}, company={company}")
    
    # メールアドレス形式チェック
    is_valid_email, email_message = validate_email(email)
    print(f"メールバリデーション: {is_valid_email}, {email_message}")
    if not is_valid_email:
        return False, email_message
    
    # パスワード強度チェック
    is_valid_pw, pw_message = validate_password(password)
    print(f"パスワードバリデーション: {is_valid_pw}, {pw_message}")
    if not is_valid_pw:
        return False, pw_message
    
    # データベースで重複チェック
    if check_user_exists_in_db(email):
        print(f"重複エラー: {email} は既に登録済み")
        return False, "このメールアドレスは既に登録されています。"
    
    # 基本認証情報も確認（重複チェック）
    base_credentials = load_credentials()
    if email in base_credentials['credentials']['usernames']:
        print(f"重複エラー: {email} は基本認証情報に既に存在")
        return False, "このメールアドレスは既に登録されています。"
    
    # 正しいハッシュ化方法（bcrypt直接使用）
    import bcrypt
    hashed_pw = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    
    # データベースにユーザーを追加
    save_result = add_user_to_db(email, name, company, hashed_pw)
    print(f"保存結果: {save_result}")
    if save_result:
        print(f"ユーザー追加成功: {email}")
        return True, "アカウントを追加しました。"
    else:
        print(f"ユーザー追加失敗: {email}")
        return False, "アカウントの保存に失敗しました。"



def merge_credentials(base_credentials, dynamic_users):
    """
    基本認証情報と動的ユーザー情報を統合する
    """
    merged_credentials = base_credentials.copy()
    
    # デバッグ情報
    print(f"基本認証情報ユーザー数: {len(base_credentials['credentials']['usernames'])}")
    print(f"動的ユーザー数: {len(dynamic_users.get('users', {}))}")
    
    # 動的ユーザーを基本認証情報に追加
    for email, user_data in dynamic_users.get("users", {}).items():
        print(f"動的ユーザー追加: {email} - {user_data.get('name', 'N/A')}")
        # streamlit-authenticatorが期待する形式（基本認証ユーザーと同じ辞書形式）
        merged_credentials["credentials"]["usernames"][email] = user_data
    
    print(f"統合後ユーザー数: {len(merged_credentials['credentials']['usernames'])}")
    print(f"統合後ユーザー一覧: {list(merged_credentials['credentials']['usernames'].keys())}")
    
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

# データベース初期化
init_database()

# 基本認証情報と動的ユーザー情報を統合
base_credentials = load_credentials()
dynamic_users = load_users_from_db()
credentials_config = merge_credentials(base_credentials, dynamic_users)

# デバッグ情報
total_users = len(credentials_config['credentials']['usernames'])
dynamic_count = len(dynamic_users.get('users', {}))
print(f"認証情報統合: 総ユーザー数={total_users}, 動的ユーザー数={dynamic_count}")

# 詳細デバッグ情報
print("=== 認証情報詳細 ===")
print(f"基本認証ユーザー: {list(base_credentials['credentials']['usernames'].keys())}")
print(f"動的ユーザー: {list(dynamic_users.get('users', {}).keys())}")
print(f"統合後ユーザー: {list(credentials_config['credentials']['usernames'].keys())}")

# 基本認証情報の形式を確認
print("=== 基本認証情報の形式確認 ===")
for email, user_data in base_credentials['credentials']['usernames'].items():
    print(f"基本ユーザー - {email}:")
    print(f"  データ型: {type(user_data)}")
    print(f"  データ内容: {user_data}")

# 動的ユーザーの詳細情報
for email, password in dynamic_users.get('users', {}).items():
    print(f"動的ユーザー詳細 - {email}:")
    user_info = dynamic_users.get('user_info', {}).get(email, {})
    print(f"  名前: {user_info.get('name', 'N/A')}")
    print(f"  会社: {user_info.get('company', 'N/A')}")
    print(f"  パスワード長: {len(password)}")
    print(f"  パスワード先頭: {password[:20]}...")



authenticator = stauth.Authenticate(
    credentials=credentials_config['credentials'],
    cookie_name=credentials_config['cookie']['name'],
    key=credentials_config['cookie']['key'],
    expiry_days=credentials_config['cookie']['expiry_days'],
    preauthorized=credentials_config['preauthorized']
)
st.set_page_config(page_title="受注集計アプリ（アグリライブ）", layout="wide")

# 自動更新機能
if st.button("🔄 データを更新", key="refresh_data"):
    st.rerun()

st.image("会社ロゴ.png", width=220)
st.title("受注集計アプリ（アグリライブ）")

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
        # デバッグ情報をセッション状態に保存
        debug_info = {
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "email": new_email,
            "name": new_name,
            "company": new_company,
            "password_length": len(new_password) if new_password else 0,
            "agree_terms": agree_terms,
            "all_fields_filled": bool(new_email and new_name and new_company and new_password)
        }
        st.session_state.debug_info = debug_info
        
        if not agree_terms:
            st.sidebar.warning("利用規約・プライバシーポリシーに同意が必要です。")
        elif new_email and new_name and new_company and new_password:
            st.sidebar.info("デバッグ: ユーザー追加処理を開始")
            ok, msg = add_user(new_email, new_name, new_company, new_password)
            
            # 結果をセッション状態に保存
            st.session_state.registration_result = {
                "success": ok,
                "message": msg,
                "timestamp": datetime.now().strftime("%H:%M:%S")
            }
            
            if ok:
                st.sidebar.success(msg)
                st.sidebar.info("アカウントが追加されました。ページを再読み込みしてログインしてください。")
                # ページを再読み込み
                st.rerun()
            else:
                st.sidebar.error(msg)
        else:
            st.sidebar.warning("すべて入力してください。")
            st.session_state.registration_result = {
                "success": False,
                "message": "入力項目が不足しています",
                "timestamp": datetime.now().strftime("%H:%M:%S")
            }

    # デバッグ情報表示エリア
    if hasattr(st.session_state, 'debug_info') and st.session_state.debug_info:
        st.sidebar.markdown("---")
        st.sidebar.subheader("🔍 デバッグ情報")
        debug = st.session_state.debug_info
        st.sidebar.info(f"**時刻**: {debug['timestamp']}")
        st.sidebar.info(f"**メール**: {debug['email']}")
        st.sidebar.info(f"**名前**: {debug['name']}")
        st.sidebar.info(f"**会社**: {debug['company']}")
        st.sidebar.info(f"**パスワード長**: {debug['password_length']}")
        st.sidebar.info(f"**利用規約同意**: {debug['agree_terms']}")
        st.sidebar.info(f"**全項目入力**: {debug['all_fields_filled']}")
        
        # デバッグ情報クリアボタン
        if st.sidebar.button("デバッグ情報をクリア", key="clear_debug"):
            st.session_state.debug_info = None
            st.session_state.registration_result = None
            st.rerun()

    # 登録結果表示エリア
    if hasattr(st.session_state, 'registration_result') and st.session_state.registration_result:
        st.sidebar.markdown("---")
        st.sidebar.subheader("📋 登録結果")
        result = st.session_state.registration_result
        if result['success']:
            st.sidebar.success(f"✅ {result['message']}")
            st.sidebar.info(f"時刻: {result['timestamp']}")
        else:
            st.sidebar.error(f"❌ {result['message']}")
            st.sidebar.info(f"時刻: {result['timestamp']}")

# --- ログインフォームを描画（必ずここで表示！） ---
# デバッグ情報を表示
if st.session_state.get('debug_info'):
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 認証デバッグ情報")
    st.sidebar.info(f"**認証状態**: {st.session_state.get('authentication_status')}")
    st.sidebar.info(f"**ユーザー名**: {st.session_state.get('username')}")
    st.sidebar.info(f"**名前**: {st.session_state.get('name')}")

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
    
    # LINE注文データの表示
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
                    file_name=f"ユーザー一覧_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("ユーザーが登録されていません。")
            
            # LINE注文データ情報
            st.subheader("📱 LINE注文データ情報")
            
            all_line_orders = get_all_line_orders()
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("総LINE注文数", len(all_line_orders))
            with col2:
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
        
        # ファイルパス情報を表示
        st.sidebar.markdown("---")
        st.sidebar.subheader("ファイルパス情報")
        import os
        current_dir = os.getcwd()
        st.sidebar.info(f"現在のディレクトリ: {current_dir}")
        
        # 主要ファイルの存在確認
        files_to_check = [
            "dynamic_users.json",
            "credentials.json", 
            "app.py"
        ]
        
        for file in files_to_check:
            file_path = os.path.join(current_dir, file)
            exists = os.path.exists(file_path)
            status = "✅ 存在" if exists else "❌ 不存在"
            st.sidebar.info(f"{file}: {status}")
            
            if exists:
                try:
                    size = os.path.getsize(file_path)
                    st.sidebar.info(f"  - サイズ: {size} bytes")
                except:
                    pass

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
                            
                            # 保存後のデータ確認
                            st.info("保存後のデータ確認:")
                            orders_file = os.path.join(LINE_ORDERS_DIR, "orders.json")
                            if os.path.exists(orders_file):
                                with open(orders_file, "r", encoding="utf-8") as f:
                                    all_orders = json.load(f)
                                st.info(f"- 全注文データ数: {len(all_orders)}")
                                for i, order in enumerate(all_orders[-3:]):  # 最新3件
                                    st.info(f"- 注文{i+1}: line_account={order.get('line_account')}, sender_name={order.get('sender_name')}")
                            
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
            
            # 一括解析ボタンを追加
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("🚀 一括解析開始", type="primary", key="batch_parse"):
                    try:
                        with st.spinner(f"{len(unprocessed_orders)}件のLINE注文を一括解析中..."):
                            processed_count = 0
                            error_count = 0
                            
                            for order in unprocessed_orders:
                                try:
                                    image_path = os.path.join(LINE_ORDERS_DIR, order['image_filename'])
                                    if os.path.exists(image_path):
                                        # OpenAI APIで解析
                                        parsed_data = parse_line_order_with_openai(
                                            image_path, 
                                            order['sender_name'], 
                                            order.get('message_text', ''),
                                            order['order_date'] # 受信日時を渡す
                                        )
                                        
                                        # 解析結果を保存
                                        success, message = save_parsed_line_order_data(order['timestamp'], parsed_data)
                                        if not success:
                                            st.error(f"解析結果の保存に失敗: {message}")
                                        
                                        processed_count += 1
                                    else:
                                        error_count += 1
                                except Exception as e:
                                    error_count += 1
                                    st.error(f"解析エラー ({order['sender_name']}): {e}")
                            
                            st.success(f"一括解析完了！ 成功: {processed_count}件, エラー: {error_count}件")
                            st.rerun()
                    except Exception as e:
                        st.error(f"一括解析エラー: {e}")
            
            with col2:
                if st.button("🗑️ 未処理データ一括削除", type="secondary", key="batch_delete"):
                    try:
                        deleted_count = 0
                        for order in unprocessed_orders:
                            success, message = delete_line_order_by_timestamp(order['timestamp'])
                            if success:
                                deleted_count += 1
                        
                        st.success(f"未処理データを {deleted_count} 件削除しました。")
                        st.rerun()
                    except Exception as e:
                        st.error(f"一括削除エラー: {e}")
            
            st.markdown("---")
            
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
                                        order.get('message_text', ''),
                                        order['order_date'] # 受信日時を渡す
                                    )
                                    
                                    # 標準形式に変換
                                    records = []
                                    delivery_date = parsed_data.get("delivery_date", "")
                                    items = parsed_data.get("items", [])
                                    
                                    for item in items:
                                        record = {
                                            "order_id": item.get("order_id", ""),
                                            "order_date": order['order_date'],  # Webアプリでの受信日を使用
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
                                    
                                    # 解析結果を保存
                                    success, message = save_parsed_line_order_data(order['timestamp'], parsed_data)
                                    if not success:
                                        st.error(f"解析結果の保存に失敗: {message}")
                                    
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
    
    # セッション状態の初期化（ファイルアップローダーの前に配置）
    if 'data_edited' not in st.session_state:
        st.session_state.data_edited = False
    
    if 'processed_files' not in st.session_state:
        st.session_state.processed_files = set()
    
    if 'parsed_records' not in st.session_state:
        st.session_state.parsed_records = []
    
    # PDF画像表示設定
    col1, col2 = st.columns([3, 1])
    with col1:
        uploaded_files = st.file_uploader(
            label="Infomart / IPORTER / PDF 等の注文ファイルをここにドラッグ＆ドロップまたは選択してください",
            accept_multiple_files=True,
            type=['txt', 'csv', 'xlsx', 'pdf'],
            key="file_uploader"
        )
        # 新しいファイルがアップロードされた場合のみ編集状態をリセット
        if uploaded_files:
            new_files_count = 0
            for file in uploaded_files:
                file_hash = f"{file.name}_{file.size}_{file.type}"
                if file_hash not in st.session_state.processed_files:
                    new_files_count += 1
            
            if new_files_count > 0:
                st.session_state.data_edited = False
    with col2:
        show_pdf_images = st.checkbox("PDF画像を表示", value=True, help="PDFファイルの画像を表示するかどうかを設定します")
        
        # 解析済みファイルリセットボタン
        if st.button("🔄 解析済みファイルをリセット", key="reset_processed_files", help="解析済みファイルの履歴をクリアします"):
            st.session_state.processed_files = set()
            st.session_state.data_edited = False
            st.session_state.parsed_records = []  # 解析済みデータもクリア
            st.success("解析済みファイルをリセットしました。")
            st.rerun()

    records = []
    debug_details = []
    
    # LINE注文データを取得（スコープ外でも使用するため、ここで定義）
    line_orders = get_line_orders_for_user(username)
    processed_line_orders = [order for order in line_orders if order.get("processed", False)]
    
    # 編集済みの場合は再解析をスキップ
    if not st.session_state.data_edited:
        # 既存の解析済みデータを取得
        records = st.session_state.parsed_records.copy()
        
        # 全データ表示機能を追加
        if processed_line_orders:
            st.subheader("📱 解析済みLINE注文データ")
            
            # 統計情報
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("解析済みLINE注文", len(processed_line_orders))
            with col2:
                st.metric("送信者数", len(set(order['sender_name'] for order in processed_line_orders)))
            with col3:
                st.metric("最新更新", max(order['order_date'] for order in processed_line_orders) if processed_line_orders else "なし")
            
            # 解析済みデータの詳細表示
            with st.expander("📋 解析済みLINE注文詳細", expanded=False):
                for i, order in enumerate(processed_line_orders):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.write(f"**{i+1}. {order['sender_name']} - {order['order_date']}**")
                        st.write(f"受信日時: {order['timestamp']}")
                        if order.get('message_text'):
                            st.write(f"メッセージ: {order['message_text']}")
                    
                    with col2:
                        # 画像表示
                        image_path = os.path.join(LINE_ORDERS_DIR, order['image_filename'])
                        if os.path.exists(image_path):
                            st.image(image_path, caption="LINE注文画像", width=200)
                    
                    st.markdown("---")
        
        # LINE注文データをrecordsに追加（まだ追加されていない場合のみ）
        existing_line_sources = {record.get("data_source", "") for record in records}
        for order in processed_line_orders:
            line_source = f"LINE注文_{order['timestamp']}"
            if line_source not in existing_line_sources:
                # 処理済みのLINE注文データをrecordsに追加
                st.info(f"処理済みLINE注文データ: {order['sender_name']} - {order['order_date']}")
                
                # 保存された解析結果を取得
                parsed_data = order.get('parsed_data')
                if parsed_data:
                    # 解析結果から商品情報を取得
                    delivery_date = parsed_data.get("delivery_date", order['order_date'])
                    items = parsed_data.get("items", [])
                    
                    for item in items:
                        record = {
                            "order_id": f"LINE_{order['timestamp']}",
                            "order_date": order['order_date'],
                            "delivery_date": delivery_date,
                            "partner_name": parsed_data.get("partner_name", order['sender_name']),
                            "product_code": item.get("product_code", ""),
                            "product_name": item.get("product_name", ""),
                            "quantity": item.get("quantity", ""),
                            "unit": item.get("unit", ""),
                            "unit_price": item.get("unit_price", ""),
                            "amount": item.get("amount", ""),
                            "remark": item.get("remark", ""),
                            "data_source": line_source
                        }
                        records.append(record)
                else:
                    # 解析結果がない場合はダミーデータを追加
                    record = {
                        "order_id": f"LINE_{order['timestamp']}",
                        "order_date": order['order_date'],
                        "delivery_date": order['order_date'],
                        "partner_name": order['sender_name'],
                        "product_code": "",
                        "product_name": "LINE注文データ（解析結果なし）",
                        "quantity": "",
                        "unit": "",
                        "unit_price": "",
                        "amount": "",
                        "remark": f"LINE注文 - {order['timestamp']}",
                        "data_source": line_source
                    }
                    records.append(record)
        
        if uploaded_files:
            # 新しいファイルのみを処理
            new_files = []
            for file in uploaded_files:
                file_hash = f"{file.name}_{file.size}_{file.type}"
                if file_hash not in st.session_state.processed_files:
                    new_files.append(file)
                    st.session_state.processed_files.add(file_hash)
            
            if new_files:
                st.info(f"新しいファイル {len(new_files)} 件を解析します")
                
                for file in new_files:
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
                                try:
                                    mitsubishi_records = parse_mitsubishi(file_like, filename)
                                    records += mitsubishi_records
                                    st.success(f"{filename} の解析が完了しました")
                                except Exception as parse_error:
                                    st.error(f"{filename} の解析に失敗しました: {parse_error}")
                                    # ログから詳細情報を取得
                                    import logging
                                    logger = logging.getLogger('parser_mitsubishi')
                                    if logger.handlers:
                                        for handler in logger.handlers:
                                            if hasattr(handler, 'baseFilename'):
                                                st.info(f"詳細ログ: {handler.baseFilename}")
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
            else:
                st.info("📝 すべてのファイルが既に解析済みです。新しいファイルをアップロードしてください。")
        
        # 解析済みデータをセッションに保存
        st.session_state.parsed_records = records
    else:
        # 編集済みの場合は既存のデータを表示
        st.info("📝 データが編集されています。ファイルを再アップロードすると再解析されます。")
        if st.button("🔄 データを再読み込み", key="reload_data"):
            st.session_state.data_edited = False
            st.rerun()
        # 編集済みの場合も既存のデータを使用
        records = st.session_state.parsed_records.copy()

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
                hide_index=True,
                on_change=lambda: setattr(st.session_state, 'data_edited', True)
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
                        # セッションの解析済みデータもクリア
                        st.session_state.parsed_records = []
                        st.rerun()
                    else:
                        st.error(message)
    else:
        st.info("注文ファイルをアップロードしてください")

elif st.session_state.get("authentication_status") is False:
    st.error("ユーザー名またはパスワードが正しくありません。")
elif st.session_state.get("authentication_status") is None:
    st.warning("ログイン情報を入力してください。")

# --- SQLiteデータベース管理 ---
def init_database():
    """SQLiteデータベースを初期化する"""
    import sqlite3
    import os
    
    # データベースファイルパス
    db_path = '/tmp/users.db' if os.getenv('RENDER') else 'users.db'
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # ユーザーテーブル作成
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            email TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            company TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"データベース初期化完了: {db_path}")

def add_user_to_db(email, name, company, password_hash):
    """データベースにユーザーを追加する"""
    import sqlite3
    import os
    
    db_path = '/tmp/users.db' if os.getenv('RENDER') else 'users.db'
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO users (email, name, company, password_hash)
            VALUES (?, ?, ?, ?)
        ''', (email, name, company, password_hash))
        
        conn.commit()
        conn.close()
        print(f"ユーザー追加成功（DB）: {email}")
        return True
    except sqlite3.IntegrityError:
        print(f"ユーザー {email} は既に存在します")
        return False
    except Exception as e:
        print(f"データベースエラー: {e}")
        return False

def load_users_from_db():
    """データベースから全ユーザーを読み込む"""
    import sqlite3
    import os
    
    db_path = '/tmp/users.db' if os.getenv('RENDER') else 'users.db'
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT email, name, company, password_hash FROM users')
        users = cursor.fetchall()
        
        conn.close()
        
        # streamlit-authenticator形式に変換
        dynamic_users = {"users": {}}
        for email, name, company, password_hash in users:
            dynamic_users["users"][email] = {
                "email": email,
                "name": name,
                "company": company,
                "password": password_hash
            }
        
        print(f"データベースから読み込み: {len(users)} ユーザー")
        return dynamic_users
    except Exception as e:
        print(f"データベース読み込みエラー: {e}")
        return {"users": {}}

def check_user_exists_in_db(email):
    """データベースでユーザーの存在を確認する"""
    import sqlite3
    import os
    
    db_path = '/tmp/users.db' if os.getenv('RENDER') else 'users.db'
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT email FROM users WHERE email = ?', (email,))
        result = cursor.fetchone()
        
        conn.close()
        return result is not None
    except Exception as e:
        print(f"データベース確認エラー: {e}")
        return False
