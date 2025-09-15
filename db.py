from pathlib import Path
import sqlite3
import hashlib
import datetime
from contextlib import contextmanager
import pandas as pd
from config import load_config

# データベースファイルのパス設定（APP_DATA_DIRを使用）
CONFIG = load_config()
DATA_DIR = Path(CONFIG.get("app_data_dir"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "app.db"

# 日本語列名→英語列名のマッピング
J2E = {
    "伝票番号": "order_id",
    "発注日": "order_date",
    "納品日": "delivery_date",
    "取引先名": "partner_name",
    "商品コード": "product_code",
    "商品名": "product_name",
    "サイズ": "size",  # 追加
    "数量": "quantity",
    "単位": "unit",
    "単価": "unit_price",
    "金額": "amount",
    "備考": "remark",
    "データ元": "data_source",
}

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """日本語/英語いずれの列名でも受け取り、英語スキーマに正規化"""
    if not isinstance(df, pd.DataFrame):
        raise ValueError("save_order_lines() には DataFrame を渡してください")

    # 日本語→英語に寄せる（存在するものだけ）
    need_rename = any(col in J2E for col in df.columns)
    df2 = df.rename(columns={k: v for k, v in J2E.items() if k in df.columns}) if need_rename else df.copy()

    # 必須列を欠けなく用意
    cols = ["order_id","order_date","delivery_date","partner_name",
            "product_code","product_name","size","quantity","unit",
            "unit_price","amount","remark","data_source"]
    for c in cols:
        if c not in df2.columns:
            df2[c] = None

    # 型のゆるやかな整形（失敗はNaN→後でNoneになる）
    for num in ["quantity","unit_price","amount"]:
        df2[num] = pd.to_numeric(df2[num], errors="coerce")
    for dcol in ["order_date","delivery_date"]:
        df2[dcol] = pd.to_datetime(df2[dcol], errors="coerce").dt.strftime("%Y/%m/%d")

    return df2

@contextmanager
def _conn():
    """データベース接続のコンテキストマネージャー"""
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def init_db():
    """データベースの初期化（テーブル作成）"""
    with _conn() as c:
        # 注文明細テーブル
        c.execute("""
        CREATE TABLE IF NOT EXISTS order_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            order_id TEXT, 
            order_date TEXT, 
            delivery_date TEXT, 
            partner_name TEXT,
            product_code TEXT, 
            product_name TEXT, 
            size TEXT,                 -- 追加
            quantity REAL, 
            unit TEXT,
            unit_price REAL, 
            amount REAL, 
            remark TEXT, 
            data_source TEXT,
            row_hash TEXT UNIQUE,
            created_at TEXT NOT NULL
        );
        """)
        
        # --- 既存DB移行（size列がなければ追加） ---
        cols = [r[1] for r in c.execute("PRAGMA table_info(order_lines)").fetchall()]
        if "size" not in cols:
            c.execute("ALTER TABLE order_lines ADD COLUMN size TEXT;")
        
        # バッチ管理テーブル
        c.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            batch_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            note TEXT
        );
        """)

def _calc_hash(row: dict) -> str:
    """行データのハッシュ値を計算（重複防止用）"""
    keys = ["order_id", "order_date", "delivery_date", "partner_name",
            "product_code", "product_name", "size", "quantity", "unit",
            "unit_price", "amount", "remark", "data_source", "batch_id"]
    s = "|".join(str(row.get(k, "")) for k in keys)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def save_order_lines(df, batch_id: str, note: str = None):
    """注文明細をデータベースに保存"""
    now = datetime.datetime.now().isoformat(timespec="seconds")
    init_db()
    df = _normalize_df(df)  # ★追加：英語スキーマに統一
    
    with _conn() as c:
        # バッチ登録（なければ）
        c.execute("INSERT OR IGNORE INTO batches(batch_id, created_at, note) VALUES(?, ?, ?)",
                  (batch_id, now, note))
        
        # 行をINSERT（重複はrow_hashのUNIQUEで無視）
        cols = ["order_id", "order_date", "delivery_date", "partner_name",
                "product_code", "product_name", "size", "quantity", "unit",
                "unit_price", "amount", "remark", "data_source"]
        
        for _, r in df.iterrows():
            row = {k: (r.get(k) if k in df.columns else None) for k in cols}
            row["batch_id"] = batch_id
            h = _calc_hash(row)
            
            c.execute("""
            INSERT OR IGNORE INTO order_lines
            (batch_id, order_id, order_date, delivery_date, partner_name,
             product_code, product_name, size, quantity, unit, unit_price, amount, remark, data_source,
             row_hash, created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (batch_id, row["order_id"], row["order_date"], row["delivery_date"], row["partner_name"],
                  row["product_code"], row["product_name"], row["size"], row["quantity"], row["unit"],
                  row["unit_price"], row["amount"], row["remark"], row["data_source"],
                  h, now))

def list_batches():
    """保存済みバッチの一覧を取得"""
    init_db()
    with _conn() as c:
        cur = c.execute("SELECT batch_id, created_at, COALESCE(note,'') FROM batches ORDER BY created_at DESC")
        return cur.fetchall()

def load_batch(batch_id: str):
    """指定されたバッチIDのデータを取得"""
    init_db()
    with _conn() as c:
        cur = c.execute("""
        SELECT order_id as '伝票番号', order_date as '発注日', delivery_date as '納品日', partner_name as '取引先名',
               product_code as '商品コード', product_name as '商品名', size as 'サイズ', quantity as '数量', unit as '単位',
               unit_price as '単価', amount as '金額', remark as '備考', data_source as 'データ元'
        FROM order_lines
        WHERE batch_id = ?
        """, (batch_id,))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)

def get_batch_stats():
    """バッチ統計情報を取得"""
    init_db()
    with _conn() as c:
        # 総バッチ数
        total_batches = c.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
        
        # 総注文行数
        total_lines = c.execute("SELECT COUNT(*) FROM order_lines").fetchone()[0]
        
        # 最新バッチ
        latest_batch = c.execute("SELECT batch_id, created_at FROM batches ORDER BY created_at DESC LIMIT 1").fetchone()
        
        return {
            "total_batches": total_batches,
            "total_lines": total_lines,
            "latest_batch": latest_batch
        }
