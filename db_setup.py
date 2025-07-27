import sqlite3
import json

# SQLiteデータベースに接続（なければ作成される）
conn = sqlite3.connect("orders.db")
cur = conn.cursor()

# 注文情報テーブルを作成（存在しなければ）
cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        source TEXT,
        order_date TEXT,
        delivery_date TEXT,        -- ★納品日を追加！
        partner_name TEXT,
        total_items INTEGER
    )
""")

# 注文商品の明細テーブルを作成（存在しなければ）
cur.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT,
        product_code TEXT,
        product_name TEXT,
        quantity_ordered TEXT,
        unit TEXT,
        quantity_received TEXT,
        unit_price TEXT,
        amount TEXT,
        amount_with_tax TEXT,     -- ★税込金額も格納
        tax_type TEXT,            -- ★税区分など必要なら追加
        note TEXT,                -- ★備考など追加可
        FOREIGN KEY(order_id) REFERENCES orders(order_id)
    )
""")

# JSONファイルを読み込み（standardized_data.json）
with open("standardized_data.json", "r", encoding="utf-8") as f:
    orders = json.load(f)

# データをデータベースに挿入
for order in orders:
    order_id = order["order_id"]
    source = order.get("source", "")
    order_date = order.get("order_date", "")
    delivery_date = order.get("delivery_date", "")     # ★納品日
    partner_name = order.get("partner_name", "")
    items = order.get("items", [])

    # 注文情報を登録
    cur.execute(
        "INSERT OR REPLACE INTO orders (order_id, source, order_date, delivery_date, partner_name, total_items) VALUES (?, ?, ?, ?, ?, ?)",
        (order_id, source, order_date, delivery_date, partner_name, len(items))
    )

    # 商品明細を登録
    for item in items:
        cur.execute(
            "INSERT INTO order_items (order_id, product_code, product_name, quantity_ordered, unit, quantity_received, unit_price, amount, amount_with_tax, tax_type, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                order_id,
                item.get("product_code", ""),
                item.get("product_name", ""),
                item.get("quantity_ordered", ""),
                item.get("unit", ""),
                item.get("quantity_received", ""),
                item.get("unit_price", ""),
                item.get("amount", ""),               # 金額（税抜）
                item.get("amount_with_tax", ""),      # 金額（税込）
                item.get("tax_type", ""),
                item.get("note", "")
            )
        )

# コミットして保存
conn.commit()
conn.close()

print("注文情報と商品明細を orders.db に保存しました。")
