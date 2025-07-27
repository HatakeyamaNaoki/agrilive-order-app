import os
from dotenv import load_dotenv

# 環境変数ファイルを読み込み（ローカル開発時のみ）
if not os.getenv('RENDER'):
    load_dotenv()

def get_openai_api_key():
    """
    OpenAI APIキーを取得する
    本番環境: Render Secrets Filesからのみ
    ローカル環境: .envファイルから
    """
    # 本番環境（Render）の場合
    if os.getenv('RENDER'):
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise Exception("本番環境でOPENAI_API_KEYが設定されていません。Render Secrets Filesを確認してください。")
        return api_key
    
    # ローカル開発環境の場合
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise Exception("ローカル環境でOPENAI_API_KEYが設定されていません。.envファイルを確認してください。")
    return api_key

def is_production():
    """
    本番環境かどうかを判定
    """
    return os.getenv('RENDER', False) or os.getenv('PRODUCTION', False) 