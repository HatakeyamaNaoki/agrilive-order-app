import csv
import io

def parse_infomart(file_buffer, filename):
    """
    Infomart CSVファイルのfile_bufferとファイル名を受け取り、
    [{...}] 形式の注文明細リストとして返す。
    """
    orders = []
    # file_bufferをShift_JISでテキストとしてラップ
    file_buffer.seek(0)
    reader = csv.reader(io.TextIOWrapper(file_buffer, encoding="shift_jis"))
    next(reader)  # 1行目は空白なのでスキップ
    header = next(reader)  # 2行目がヘッダー

    # 列インデックス取得（列名は全角カギカッコに注意）
    idx_order_id = header.index("［伝票No］")
    idx_order_date = header.index("［発注日］")
    idx_delivery_date = header.index("［納品日］")
    idx_partner_name = header.index("［取引先名］")
    idx_product_code = header.index("［自社管理商品コード］")
    idx_product_name = header.index("［商品名］")
    idx_quantity = header.index("［数量］")
    idx_unit = header.index("［単位］")
    idx_unit_price = header.index("［単価］")
    idx_total_price = header.index("［金額］")
    idx_remark = header.index("［規格］")

    # 明細ごとに1件ずつ出力（1注文1行につき1明細で"items"にしない形）
    for row in reader:
        # F判定はA列を直接見る
        if len(row) > 0 and str(row[0]).strip() == "F":
            break
        # 空行や列数不足スキップ
        if not row or len(row) <= max(
            idx_order_id, idx_order_date, idx_delivery_date, idx_partner_name, idx_product_code, idx_remark
        ):
            continue

        # 注文明細をフラットに出力
        orders.append({
            "order_id": row[idx_order_id],
            "order_date": row[idx_order_date],
            "delivery_date": row[idx_delivery_date],
            "partner_name": row[idx_partner_name],
            "product_code": row[idx_product_code],
            "product_name": row[idx_product_name],
            "size": "",  # 追加：Infomartはサイズ空
            "quantity": row[idx_quantity],
            "unit": row[idx_unit],
            "unit_price": row[idx_unit_price],
            "amount": row[idx_total_price],
            "remark": row[idx_remark],
            "data_source": filename,   # ファイル名を記録
        })
    return orders
