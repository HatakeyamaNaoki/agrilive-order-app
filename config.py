import os
import json
import logging
from dotenv import load_dotenv
from pathlib import Path

# ロガーの設定
logger = logging.getLogger(__name__)

# 環境変数ファイルを読み込み（ローカル開発時のみ）
if not os.getenv('RENDER'):
    load_dotenv()



def is_production():
    """本番環境かどうかを判定"""
    return os.getenv('ENV') == 'production' or os.getenv('RENDER') == 'true'

def get_openai_api_key():
    """OpenAI APIキーを取得"""
    # 本番環境では環境変数から取得
    if is_production():
        return os.getenv('OPENAI_API_KEY')
    
    # 開発環境では.envファイルから取得
    try:
        from dotenv import load_dotenv
        load_dotenv()
        return os.getenv('OPENAI_API_KEY')
    except ImportError:
        return os.getenv('OPENAI_API_KEY')

def get_line_channel_access_token():
    """LINE Channel Access Tokenを取得"""
    return os.getenv('LINE_CHANNEL_ACCESS_TOKEN')

# 顧客ごとの出力設定JSONを読み込む関数
def load_config(user_id=None):
    """ユーザー設定を読み込み"""
    config = {
        'user_id': user_id,
        'is_production': is_production(),
        'app_data_dir': os.getenv('APP_DATA_DIR', str(Path(__file__).parent / 'data'))
    }
    return config
