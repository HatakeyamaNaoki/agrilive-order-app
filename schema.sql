-- user_systems（ユーザーが使用可能な受発注システム）
CREATE TABLE IF NOT EXISTS user_systems (
    username TEXT,
    system_name TEXT
);

-- orders（注文ヘッダー）
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    source TEXT,
    order_date TEXT,
    partner_name TEXT,
    total_items INTEGER
);

-- order_items（明細行）
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
    FOREIGN KEY(order_id) REFERENCES orders(order_id)
);
