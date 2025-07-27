import streamlit as st
import streamlit_authenticator as stauth
import json
import pandas as pd
import io
import datetime
import pytz
from config import get_openai_api_key, is_production
from config_loader import load_config
from parser_infomart import parse_infomart
from parser_iporter import parse_iporter
from parser_mitsubishi import parse_mitsubishi
from parser_pdf import parse_pdf_handwritten
from docx import Document

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
st.image("会社ロゴ.png", width=220)
st.title("受発注データ集計アプリ（アグリライブ）")

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
    
    # 管理者ダッシュボード
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
        
        # システム情報
        st.subheader("⚙️ システム情報")
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"**環境**: {'本番環境' if is_production() else '開発環境'}")
            st.info(f"**現在のユーザー**: {username}")
        
        with col2:
            st.info(f"**基本認証ファイル**: credentials.json")
            st.info(f"**動的ユーザーファイル**: dynamic_users.json")
        
        return  # 管理者ダッシュボード表示時は通常の機能をスキップ

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

    st.subheader("注文データファイルのアップロード")
    uploaded_files = st.file_uploader(
        label="Infomart / IPORTER / PDF 等の注文データファイルをここにドラッグ＆ドロップまたは選択してください",
        accept_multiple_files=True,
        type=['txt', 'csv', 'xlsx', 'pdf']
    )

    records = []
    debug_details = []
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
        now_str = datetime.datetime.now(jst).strftime("%y%m%d_%H%M")

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
        st.download_button(
            label="Excelをダウンロード",
            data=output,
            file_name=f"{now_str}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("注文ファイルをアップロードしてください")

elif st.session_state.get("authentication_status") is False:
    st.error("ユーザー名またはパスワードが正しくありません。")
elif st.session_state.get("authentication_status") is None:
    st.warning("ログイン情報を入力してください。")

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
