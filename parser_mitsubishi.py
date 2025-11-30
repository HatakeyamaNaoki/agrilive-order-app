# parser_mitsubishi.py
import pandas as pd
from datetime import datetime
import logging
from typing import Union, BinaryIO, TextIO

# ロガーの設定
logger = logging.getLogger(__name__)

def parse_mitsubishi(file_path: Union[str, BinaryIO, TextIO], file_name: str) -> list[dict]:
    df = pd.read_excel(file_path, sheet_name=0, header=None)

    # 基本情報の抽出
    try:
        denpyo_no = str(df.iloc[5, 1])
        order_date_text = df.iloc[3, 52]
        delivery_date_raw = df.iloc[5, 9]
        customer_name = f"{df.iloc[0, 52]} {df.iloc[5, 19]}"
    except Exception as e:
        error_msg = f"[基本情報の抽出エラー] {file_name}: {e}"
        logger.error(error_msg)
        raise Exception(error_msg)

    # 納品日（25/07/22 → 2025/07/22）形式変換（年推定の基準として使用）
    delivery_date = ""
    delivery_year = None
    try:
        delivery_date = datetime.strptime(str(delivery_date_raw), "%y/%m/%d").strftime("%Y/%m/%d")
        delivery_year = datetime.strptime(str(delivery_date_raw), "%y/%m/%d").year
        logger.info(f"[納品日変換成功] {file_name}: '{delivery_date_raw}' → {delivery_date}")
    except Exception:
        warning_msg = f"[納品日変換失敗] {file_name}: '{delivery_date_raw}'"
        logger.warning(warning_msg)

    # 発注日（(発注日 MM/DD) ～）から MM/DD 抽出し YYYY/MM/DD に変換
    # 納品日の年を基準に年跨ぎ誤判定を防止
    try:
        mmdd = order_date_text.split('発注日')[1].split(')')[0].strip()
        
        # 年推定ロジック
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        if delivery_year:
            # 納品日の年を基準とする
            estimated_year = delivery_year
            logger.info(f"[年推定] {file_name}: 納品日の年({delivery_year})を基準として使用")
        else:
            # 納品日の年が取得できない場合は現在年を使用
            estimated_year = current_year
            logger.info(f"[年推定] {file_name}: 現在年({current_year})を基準として使用")
        
        # 発注日を構築
        order_date_candidate = datetime.strptime(f"{estimated_year}/{mmdd}", "%Y/%m/%d")
        
        # 年跨ぎチェック：年末年始の誤判定を防止
        if delivery_date and delivery_year:
            delivery_date_obj = datetime.strptime(delivery_date, "%Y/%m/%d")
            
            # 発注日が納品日より後の場合の処理
            if order_date_candidate > delivery_date_obj:
                # 年末年始の特殊ケース（12月発注→1月納品）
                if order_date_candidate.month == 12 and delivery_date_obj.month == 1:
                    # 12月発注→1月納品は正常なケース
                    logger.info(f"[年末年始ケース] {file_name}: 12月発注→1月納品は正常として処理")
                else:
                    # その他の場合は前年として扱う
                    order_date_candidate = datetime.strptime(f"{estimated_year-1}/{mmdd}", "%Y/%m/%d")
                    logger.info(f"[年跨ぎ調整] {file_name}: 発注日を前年に調整 {estimated_year} → {estimated_year-1}")
            
            # 発注日が納品日より大幅に前の場合（例：1月発注→12月納品）
            elif (delivery_date_obj - order_date_candidate).days > 300:
                # 1年を超える差がある場合は翌年として扱う
                order_date_candidate = datetime.strptime(f"{estimated_year+1}/{mmdd}", "%Y/%m/%d")
                logger.info(f"[年跨ぎ調整] {file_name}: 発注日を翌年に調整 {estimated_year} → {estimated_year+1}")
        
        order_date = order_date_candidate.strftime("%Y/%m/%d")
        logger.info(f"[発注日変換成功] {file_name}: '{order_date_text}' → {order_date}")
        
    except Exception:
        order_date = datetime.today().strftime("%Y/%m/%d")
        warning_msg = f"[発注日変換失敗] {file_name}: '{order_date_text}' → {order_date}"
        logger.warning(warning_msg)

    result = []

    # 商品情報の抽出（Excel上11行目 = row=10 から各行をチェック）
    # 各行をチェックして、商品コードまたは商品名が存在する行をすべて処理
    # 同じ商品が連続する場合でも、すべての行を処理する
    for row in range(10, df.shape[0]):
        code_cell = df.iloc[row, 5]  # F列（商品コード）
        name_cell = df.iloc[row, 7]  # H列（商品名）
        quantity_cell = df.iloc[row, 23]  # W列（数量）
        
        # 商品コード、商品名、数量のいずれかが存在する場合は処理対象
        # すべてが空の場合はスキップ
        has_code = pd.notna(code_cell) and str(code_cell).strip() != ""
        has_name = pd.notna(name_cell) and str(name_cell).strip() != ""
        has_quantity = pd.notna(quantity_cell) and str(quantity_cell).strip() != ""
        
        # デバッグログ（最初の数行のみ）
        if row <= 12:
            logger.info(f"[行{row}] コード={code_cell}, 商品名={name_cell}, 数量={quantity_cell}, has_code={has_code}, has_name={has_name}")
        
        # 商品コードまたは商品名が存在する場合のみ処理
        # 数量が存在する場合も処理対象に含める（商品コード・商品名が空でも数量があれば処理）
        if not (has_code or has_name or has_quantity):
            if row <= 12:
                logger.info(f"[行{row}] スキップ: すべて空")
            continue

        next_row = row + 1 if row + 1 < df.shape[0] else row

        # 数量・単価を取得（新しい列番号）
        quantity_raw = df.iloc[row, 23]  # W列（23列目）
        unit_price_raw = df.iloc[row, 29]  # AC列（29列目）
        
        # 金額を計算（数量×単価）
        try:
            quantity_num = float(quantity_raw) if pd.notna(quantity_raw) else 0
            unit_price_num = float(unit_price_raw) if pd.notna(unit_price_raw) else 0
            amount_calculated = quantity_num * unit_price_num
        except (ValueError, TypeError):
            amount_calculated = 0
        
        item = {
            "order_id": denpyo_no,
            "order_date": order_date,
            "delivery_date": delivery_date,
            "partner_name": customer_name,
            "product_code": str(code_cell) if has_code else "",  # F列（5列目）
            "product_name": str(name_cell) if has_name else "",  # H列（7列目）
            "size": "",  # 追加：三菱はサイズ空
            "quantity": str(quantity_raw) if pd.notna(quantity_raw) else "",  # W列（23列目）
            "unit": str(df.iloc[row, 27]) if pd.notna(df.iloc[row, 27]) else "",  # AA列（27列目）
            "unit_price": str(unit_price_raw) if pd.notna(unit_price_raw) else "",  # AC列（29列目）
            "amount": str(amount_calculated),  # 数量×単価の計算結果
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
