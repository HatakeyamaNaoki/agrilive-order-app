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

def detect_csv_type(content_bytes):
    ENCODINGS = ["utf-8-sig", "utf-8", "cp932", "shift_jis"]
    debug_log = []
    for enc in ENCODINGS:
        try:
            file_str = content_bytes.decode(enc)
            sio = io.StringIO(file_str)
            first_line = sio.readline().strip().split(",")
            debug_log.append(f"[{enc}] first_line={first_line}")
            # ★クォート除去して比較
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
    username = st.session_state.get("username", "")
    name = st.session_state.get("name", "")
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
    debug_details = []
    if uploaded_files:
        for file in uploaded_files:
            content = file.read()
            filename = file.name
            filetype, detected_enc, debug_log = detect_csv_type(content)
            debug_details.append(f"【{filename}】\n" + "\n".join(debug_log))
            if filetype == 'infomart':
                file_like = io.BytesIO(content)
                records += parse_infomart(file_like, filename)
            elif filetype == 'iporter':
                file_like = io.BytesIO(content)
                records += parse_iporter(file_like, filename)
            else:
                st.warning(f"{filename} は未対応のフォーマットです")
    # --- デバッグ情報表示 ---
    if debug_details:
        st.info("=== デバッグログ ===\n" + "\n\n".join(debug_details))

    # --- 以下、集計・エクセル出力などは前回のまま（略） ---
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
