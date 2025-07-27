import pandas as pd

# DataFrameをJSON設定に基づいて整形する関数
def format_output(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    df: 標準化済みのDataFrame（すべての項目を含む）
    config: 顧客ごとの出力設定（JSON形式で読み込み済みの辞書）
    """
    # 出力対象の列を抽出
    columns = config.get("columns", [])
    
    # 表示順に列を並び替え（存在する列のみ）
    selected_columns = [col for col in columns if col in df.columns]
    
    # 抽出・並び替えたDataFrameを返す
    return df[selected_columns]
