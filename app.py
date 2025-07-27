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
    with open("credentials.json", "r", encoding="utf-8") as f:
        credentials = json.load(f)
    if email in credentials['credentials']['usernames']:
        return False, "このメールアドレスは既に登録されています。"
    hashed_pw = stauth.Hasher.hash(password)
    credentials['credentials']['usernames'][email] = {
        "email": email,
        "name": name,
        "company": company,
        "password": hashed_pw
    }
    with open("credentials.json", "w", encoding="utf-8") as f:
        json.dump(credentials, f, ensure_ascii=False, indent=4)
    return True, "アカウントを追加しました。"

# --- 認証 ---
with open("credentials.json", "r", encoding="utf-8") as f:
    credentials_config = json.load(f)
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
    authenticator.logout('ログアウト', 'sidebar')
    st.success(f"{name} さん、ようこそ！")
    
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
        # 本番環境の場合
        st.sidebar.markdown("---")
        st.sidebar.subheader("OpenAI API設定（本番環境）")
        
        # APIキーの状態を確認
        try:
            api_key = get_openai_api_key()
            if api_key:
                st.sidebar.success("Render Secrets FilesからAPIキーを正常に取得しています")
                # APIキーの一部を表示（デバッグ用）
                masked_key = api_key[:8] + "..." + api_key[-4:] if len(api_key) > 12 else "***"
                st.sidebar.info(f"APIキー: {masked_key}")
            else:
                st.sidebar.error("APIキーが取得できません")
        except Exception as e:
            st.sidebar.error(f"APIキー取得エラー: {e}")
            st.sidebar.info("Render Secrets Filesの設定を確認してください")
            st.sidebar.markdown("""
            **Render Secrets Files設定手順:**
            1. Renderダッシュボードでプロジェクトを開く
            2. 「Environment」タブを選択
            3. 「Secrets Files」セクションで以下を設定:
               - キー: `OPENAI_API_KEY`
               - 値: あなたのOpenAI APIキー
            4. 「Save Changes」をクリック
            5. アプリケーションを再デプロイ
            
            **注意:** Secret Filesは環境変数とは異なり、ファイルとして保存されます。
            アプリケーションのルートディレクトリまたは `/etc/secrets/` からアクセスできます。
            """)

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
