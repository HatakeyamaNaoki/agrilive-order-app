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
    remark TEXT,                   -- 備考（右段の商品名や数量情報が含まれる可能性）
    layout_type TEXT,              -- レイアウトタイプ（左右2段構成、1段構成、既印字商品名等）
    layout_confidence REAL,        -- レイアウト検知の信頼度
    is_preprinted BOOLEAN,        -- 既印字商品名かどうか
    confidence REAL,               -- 抽出の信頼度
    alternatives TEXT,             -- 代替解釈（JSON形式）
    FOREIGN KEY(order_id) REFERENCES orders(order_id)
);
