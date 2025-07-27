# parser_mitsubishi.py
import pandas as pd
from datetime import datetime
import streamlit as st  # デバッグ出力用

def parse_mitsubishi(file_path: str, file_name: str) -> list[dict]:
    df = pd.read_excel(file_path, sheet_name=0, header=None)

    # 基本情報の抽出
    try:
        denpyo_no = str(df.iloc[5, 1])
        order_date_text = df.iloc[3, 52]
        delivery_date_raw = df.iloc[5, 9]
        customer_name = f"{df.iloc[0, 52]} {df.iloc[5, 19]}"
    except Exception as e:
        st.error(f"[基本情報の抽出エラー] {file_name}: {e}")
        return []

    # 発注日（(発注日 MM/DD) ～）から MM/DD 抽出し YYYY/MM/DD に変換
    try:
        mmdd = order_date_text.split('発注日')[1].split(')')[0].strip()
        order_date = datetime.strptime(f"{datetime.now().year}/{mmdd}", "%Y/%m/%d").strftime("%Y/%m/%d")
    except Exception:
        order_date = datetime.today().strftime("%Y/%m/%d")
        st.warning(f"[発注日変換失敗] {file_name}: '{order_date_text}' → {order_date}")

    # 納品日（25/07/22 → 2025/07/22）形式変換
    try:
        delivery_date = datetime.strptime(str(delivery_date_raw), "%y/%m/%d").strftime("%Y/%m/%d")
    except Exception:
        delivery_date = ""
        st.warning(f"[納品日変換失敗] {file_name}: '{delivery_date_raw}'")

    result = []

    # 商品情報の抽出（Excel上11行目 = row=10 から2行おき）
    for row in range(10, df.shape[0], 2):
        code_cell = df.iloc[row, 5]  # F列
        if pd.isna(code_cell) or str(code_cell).strip() == "":
            continue

        next_row = row + 1 if row + 1 < df.shape[0] else row

        item = {
            "order_id": denpyo_no,
            "order_date": order_date,
            "delivery_date": delivery_date,
            "partner_name": customer_name,
            "product_code": str(df.iloc[row, 5]),
            "product_name": str(df.iloc[row, 7]),
            "quantity": str(df.iloc[row, 34]),
            "unit": str(df.iloc[row, 38]),
            "unit_price": str(df.iloc[row, 41]),
            "amount": str(df.iloc[row, 46]),
            "remark": " ".join(
                str(cell) for cell in [
                    df.iloc[row, 17],
                    df.iloc[row, 55],
                    df.iloc[next_row, 7],
                    df.iloc[next_row, 13],
                    df.iloc[next_row, 55],
                    df.iloc[next_row, 65],
                ]
                if pd.notna(cell) and str(cell).strip() != ""
            ).strip(),
            "data_source": file_name
        }
        result.append(item)

    return result
