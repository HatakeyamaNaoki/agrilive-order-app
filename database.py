import sqlite3

# データベース接続を確立する関数
def get_connection(db_path="orders.db"):
    return sqlite3.connect(db_path)

# 顧客の契約情報を取得する関数（例：使用可能なシステムなど）
def get_user_contract_info(conn, username):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT system_name FROM user_systems
        WHERE username = ?
    """, (username,))
    result = cursor.fetchall()
    return [row[0] for row in result]
