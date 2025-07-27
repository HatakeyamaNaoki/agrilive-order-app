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
        # デバッグ情報を詳細に出力
        print("=== 本番環境デバッグ情報 ===")
        print(f"RENDER環境変数: {os.getenv('RENDER')}")
        
        # まず環境変数として試行
        api_key = (os.getenv('OPENAI_API_KEY') or 
                  os.getenv('OPENAI_API_KEY_SECRET') or
                  os.getenv('OPENAI_API_KEY_SECRETS') or
                  os.getenv('OPENAI_API_KEY_RENDER') or
                  os.getenv('OPENAI_API_KEY_SECRETS_FILE'))
        
        print(f"環境変数からのAPIキー存在: {bool(api_key)}")
        
        # 環境変数で取得できない場合、Secret Filesから読み込み
        if not api_key:
            try:
                # Render Secret Filesのパスを試行
                secret_paths = [
                    '/etc/secrets/OPENAI_API_KEY',
                    'OPENAI_API_KEY',
                    './OPENAI_API_KEY'
                ]
                
                for path in secret_paths:
                    if os.path.exists(path):
                        with open(path, 'r') as f:
                            api_key = f.read().strip()
                            print(f"Secret FilesからAPIキー取得成功: {path}")
                            break
                
                if not api_key:
                    print("Secret Filesパス確認:")
                    for path in secret_paths:
                        print(f"  {path}: {os.path.exists(path)}")
                    
                    # ルートディレクトリのファイル一覧を確認
                    root_files = [f for f in os.listdir('.') if os.path.isfile(f)]
                    print(f"ルートディレクトリのファイル: {root_files}")
                    
            except Exception as e:
                print(f"Secret Files読み込みエラー: {e}")
        
        # すべての環境変数を確認（機密情報は隠す）
        all_env_vars = {k: '***' if 'KEY' in k or 'SECRET' in k or 'PASSWORD' in k else v 
                       for k, v in os.environ.items()}
        print(f"利用可能な環境変数: {all_env_vars}")
        
        if not api_key:
            # 利用可能な環境変数を確認
            env_vars = {k: v for k, v in os.environ.items() if 'OPENAI' in k or 'API' in k}
            print(f"OpenAI関連環境変数: {env_vars}")
            raise Exception("本番環境でOPENAI_API_KEYが設定されていません。Render Secrets Filesを確認してください。")
        
        print(f"APIキー取得成功: {api_key[:8]}...{api_key[-4:] if len(api_key) > 12 else '***'}")
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