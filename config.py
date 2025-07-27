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
        # 複数の環境変数名を試行
        api_key = os.getenv('OPENAI_API_KEY') or os.getenv('OPENAI_API_KEY_SECRET')
        
        # デバッグ情報（本番環境ではログに出力される）
        render_env = os.getenv('RENDER')
        api_key_exists = bool(api_key)
        print(f"Render環境: {render_env}, APIキー存在: {api_key_exists}")
        
        if not api_key:
            # 利用可能な環境変数を確認
            env_vars = {k: v for k, v in os.environ.items() if 'OPENAI' in k or 'API' in k}
            print(f"OpenAI関連環境変数: {env_vars}")
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