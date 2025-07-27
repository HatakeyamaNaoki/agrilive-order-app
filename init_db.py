import sqlite3

# schema.sqlを読み込んでデータベースを初期化
def initialize_database():
    with open("schema.sql", "r", encoding="utf-8") as f:
        schema_sql = f.read()

    conn = sqlite3.connect("orders.db")
    cursor = conn.cursor()
    cursor.executescript(schema_sql)
    conn.commit()
    conn.close()
    print("データベースを初期化しました。")

if __name__ == "__main__":
    initialize_database()
