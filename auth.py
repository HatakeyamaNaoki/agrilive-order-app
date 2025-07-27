import streamlit_authenticator as stauth
import json

# 認証設定を読み込む関数
def load_authenticator():
    with open("credentials.json", "r") as file:
        config = json.load(file)

    # streamlit-authenticatorの初期化
    authenticator = stauth.Authenticate(
        credentials=config['credentials'],
        cookie_name=config['cookie']['name'],
        key=config['cookie']['key'],
        expiry_days=config['cookie']['expiry_days'],
        preauthorized=config['preauthorized']
    )
    return authenticator
