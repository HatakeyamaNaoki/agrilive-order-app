import json
import os

# 顧客ごとの出力設定JSONを読み込む関数
def load_config(user_id: str) -> dict:
    """
    user_id: ログインユーザー名などに基づいた設定ファイル名（例："agrilive_user"）
    """
    config_path = os.path.join("configs", f"{user_id}.json")
    if not os.path.exists(config_path):
        # 設定ファイルがなければデフォルト設定を返す
        return {
            "columns": [
                "伝票番号", "発注日", "納品日", "取引先名", "商品コード",
                "商品名", "数量", "単位", "単価", "金額", "備考"
            ]
        }
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)
