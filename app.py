import streamlit as st
import streamlit_authenticator as stauth
import json
import pandas as pd
import io
import datetime
import pytz
from config_loader import load_config
from parser_infomart import parse_infomart
from parser_iporter import parse_iporter

# --- ファイル形式判定（エンコーディング自動対応） ---
def detect_csv_type(file_like):
    ENCODINGS = ["utf-8-sig", "cp932", "shift_jis"]
    content = file_like.read()
    for enc in ENCODINGS:
        try:
            file_str = content.decode(enc)
            df = pd.read_csv(io.StringIO(file_str), header=None, nrows=2)
            row1 = [str(cell).strip() for cell in df.iloc[0].tolist()]
            row2 = [str(cell).strip() for cell in df.iloc[1].tolist()] if len(df) > 1 else []
            if len(row2) > 0 and row2[0] in ['[データ区分]', '［データ区分］']:
                return 'infomart', enc
            elif len(row1) > 0 and row1[0] == '伝票番号':
                return 'iporter', enc
        except Exception:
            continue
    return 'unknown', None

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
authenticator.login(location='main')

if st.session_state.get("authentication_status"):
    username = st.session_state["username"] if "username" in st.session_state else ""
    name = st.session_state["name"] if "name" in st.session_state else ""
    config = load_config(user_id=username)
    authenticator.logout('ログアウト', 'sidebar')
    st.success(f"{name} さん、ようこそ！")

    st.subheader("注文データファイルのアップロード")
    uploaded_files = st.file_uploader(
        label="Infomart / IPORTER の注文データファイルをここにドラッグ＆ドロップまたは選択してください",
        accept_multiple_files=True,
        type=['txt', 'csv']
    )

    records = []
    if uploaded_files:
        for file in uploaded_files:
            file.seek(0)
            content = file.read()
            # 判定＆本処理で使うため、2つ複製
            file_like1 = io.BytesIO(content)
            file_like2 = io.BytesIO(content)
            filename = file.name

            file_like1.seek(0)
            filetype, detected_enc = detect_csv_type(file_like1)
            file_like2.seek(0)
            if filetype == 'infomart':
                records += parse_infomart(file_like2, filename, encoding=detected_enc)
            elif filetype == 'iporter':
                records += parse_iporter(file_like2, filename, encoding=detected_enc)
            else:
                st.warning(f"{filename} は未対応のフォーマットです")
        
    # --- 既存JSONデータ追加する場合（略） ---
    try:
        with open("standardized_data.json", "r", encoding="utf-8") as f:
            json_orders = json.load(f)
        # DataFrame化してrecordsに追加する処理も可能
    except FileNotFoundError:
        pass

    if records:
        df = pd.DataFrame(records)
        df = df.dropna(how='all')
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

        # 日付整形
        for col in ["発注日", "納品日"]:
            edited_df[col] = pd.to_datetime(edited_df[col], errors="coerce").dt.strftime("%Y/%m/%d")

        # 数量はfloat化
        edited_df["数量"] = pd.to_numeric(edited_df["数量"], errors="coerce").fillna(0)

        # ソートシート
        df_sorted = edited_df.sort_values(
            by=["商品名", "納品日", "発注日"], na_position="last"
        )

        # 集計シート
        df_agg = (
            df_sorted
            .groupby(["商品名", "備考", "単位"], dropna=False, as_index=False)
            .agg({"数量": "sum"})
        )
        df_agg = df_agg[["商品名", "備考", "数量", "単位"]]
        df_agg = df_agg.sort_values(by=["商品名"])

        # Excel出力
        output = io.BytesIO()
        jst = pytz.timezone("Asia/Tokyo")
        now_str = datetime.datetime.now(jst).strftime("%y%m%d_%H%M")
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            edited_df.to_excel(writer, index=False, sheet_name="注文一覧")
            df_sorted.to_excel(writer, index=False, sheet_name="注文一覧(層別結果)")
            df_agg.to_excel(writer, index=False, sheet_name="集計結果")

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
