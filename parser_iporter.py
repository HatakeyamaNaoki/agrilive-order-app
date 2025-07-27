import csv
import re
import io

def parse_iporter(file, filename=None):
    # --- エンコーディング自動判定 ---
    content = file.read()
    encodings = ["utf-8-sig", "cp932", "shift_jis"]
    for enc in encodings:
        try:
            text = content.decode(enc)
            break
        except Exception:
            continue
    else:
        raise RuntimeError("どのエンコーディングでもファイルが読めませんでした。")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)

    records = []
    i = 1  # 2行目（index=1）から開始
    while i < len(rows):
        main = rows[i]
        if len(main) < 54:
            i += 27
            continue
        order_id = main[0].strip()
        date_info = main[5] if len(main) > 5 else ""
        partner_name = main[7] if len(main) > 7 else ""
        # 正規表現で日付抜き出し
        m = re.search(r"発注日:(\d{4}/\d{2}/\d{2})", date_info)
        order_date = m.group(1) if m else ""
        m2 = re.search(r"納品予定日:(\d{4}/\d{2}/\d{2})", date_info)
        delivery_date = m2.group(1) if m2 else ""

        # 4～13行目が注文内容（index 3～12）
        for j in range(3, 13):
            idx = i + j - 1
            if idx >= len(rows):
                break
            row = rows[idx]
            # 商品名が空白ならそのブロックはそれ以上出力不要！
            if len(row) < 54 or not row[46].strip():
                break

            # データ取得
            record = {
                "order_id": order_id,
                "order_date": order_date,
                "delivery_date": delivery_date,
                "partner_name": partner_name,
                "product_code": row[44].strip(),
                "product_name": row[46].strip(),
                "quantity": row[47].strip(),
                "unit": row[48].strip(),
                "unit_price": row[51].replace("円", "").replace(",", "").strip(),
                "amount": row[53].replace("円", "").replace(",", "").strip(),
                "remark": row[55].strip(),
                "data_source": filename if filename else "",
                "confidence": 1.0,         # 構造化データなので高信頼度
            }
            records.append(record)
        i += 27
    
    return records
