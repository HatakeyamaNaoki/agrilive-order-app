import os
import json
import logging
from dotenv import load_dotenv

# ロガーの設定
logger = logging.getLogger(__name__)

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
                    if os.getenv("DEBUG") == "1":
                        print("Secret Filesパス確認:")
                        for path in secret_paths:
                            print(f"  {path}: {os.path.exists(path)}")
                        
                        # ルートディレクトリのファイル一覧を確認
                        root_files = [f for f in os.listdir('.') if os.path.isfile(f)]
                        print(f"ルートディレクトリのファイル: {root_files}")
                    else:
                        print("Secret Filesパス確認中...")
                    
            except Exception as e:
                print(f"Secret Files読み込みエラー: {e}")
        
        # デバッグモードでのみ環境変数を表示
        if os.getenv("DEBUG") == "1":
            # すべての環境変数を確認（機密情報は隠す）
            all_env_vars = {k: '***' if 'KEY' in k or 'SECRET' in k or 'PASSWORD' in k else v 
                for k, v in os.environ.items()}
            print(f"利用可能な環境変数: {all_env_vars}")
        else:
            print("環境変数確認完了")
        
        if not api_key:
            # デバッグモードでのみ詳細情報を表示
            if os.getenv("DEBUG") == "1":
                env_vars = {k: v for k, v in os.environ.items() if 'OPENAI' in k or 'API' in k}
                print(f"OpenAI関連環境変数: {env_vars}")
            raise Exception("本番環境でOPENAI_API_KEYが設定されていません。Render Secrets Filesを確認してください。")
        
        # デバッグモードでのみ機密情報を表示
        if os.getenv("DEBUG") == "1":
            print(f"APIキー取得成功: {api_key[:8]}...{api_key[-4:] if len(api_key) > 12 else '***'}")
        else:
            print("APIキー取得成功")
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

def get_line_channel_access_token():
    """
    LINE Channel Access Tokenを取得する
    本番環境: Render Secrets Filesからのみ
    ローカル環境: .envファイルから
    """
    # 本番環境（Render）の場合
    if os.getenv('RENDER'):
        # まず環境変数として試行
        token = (os.getenv('LINE_CHANNEL_ACCESS_TOKEN') or 
            os.getenv('LINE_CHANNEL_ACCESS_TOKEN_SECRET') or
            os.getenv('LINE_CHANNEL_ACCESS_TOKEN_SECRETS') or
            os.getenv('LINE_CHANNEL_ACCESS_TOKEN_RENDER') or
            os.getenv('LINE_CHANNEL_ACCESS_TOKEN_SECRETS_FILE'))
        
        # 環境変数で取得できない場合、Secret Filesから読み込み
        if not token:
            try:
                # Render Secret Filesのパスを試行
                secret_paths = [
                    '/etc/secrets/LINE_CHANNEL_ACCESS_TOKEN',
                    'LINE_CHANNEL_ACCESS_TOKEN',
                    './LINE_CHANNEL_ACCESS_TOKEN'
                ]
                
                for path in secret_paths:
                    if os.path.exists(path):
                        with open(path, 'r') as f:
                            token = f.read().strip()
                            print(f"Secret FilesからLINE Token取得成功: {path}")
                            break
                
                if not token:
                    if os.getenv("DEBUG") == "1":
                        print("LINE Token Secret Filesパス確認:")
                        for path in secret_paths:
                            print(f"  {path}: {os.path.exists(path)}")
                    else:
                        print("LINE Token Secret Filesパス確認中...")
                    
            except Exception as e:
                print(f"LINE Token Secret Files読み込みエラー: {e}")
        
        if not token:
            raise Exception("本番環境でLINE_CHANNEL_ACCESS_TOKENが設定されていません。Render Secrets Filesを確認してください。")
        
        # デバッグモードでのみ機密情報を表示
        if os.getenv("DEBUG") == "1":
            print(f"LINE Token取得成功: {token[:8]}...{token[-4:] if len(token) > 12 else '***'}")
        else:
            print("LINE Token取得成功")
        return token
    
    # ローカル開発環境の場合
    token = os.getenv('LINE_CHANNEL_ACCESS_TOKEN')
    if not token:
        raise Exception("ローカル環境でLINE_CHANNEL_ACCESS_TOKENが設定されていません。.envファイルを確認してください。")
    return token 

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
