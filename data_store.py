# 標準化された注文データを一時的に格納する構造体

class DataStore:
    def __init__(self):
        self.orders = []

    def load(self, order_list):
        """外部から読み込んだ注文リストを保持"""
        self.orders = order_list

    def get_all(self):
        """全注文を返す"""
        return self.orders

    def to_dataframe(self):
        """注文データを平坦化してDataFrame形式に変換"""
        import pandas as pd
        rows = []
        for order in self.orders:
            for item in order.get("items", []):
                rows.append({
                    "伝票番号": order.get("order_id"),
                    "発注日": order.get("order_date"),
                    "納品日": order.get("delivery_date"),
                    "取引先名": order.get("partner_name"),
                    "商品コード": item.get("product_code"),
                    "商品名": item.get("product_name"),
                    "数量": item.get("quantity_ordered") or item.get("quantity_received"),
                    "単位": item.get("unit"),
                    "単価": item.get("unit_price"),
                    "金額": item.get("amount_with_tax") or item.get("amount"),
                    "備考": item.get("remark", ""),
                    "データ元": order.get("source")
                })
        return pd.DataFrame(rows)