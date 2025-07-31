# Webhookサーバー起動の修正版
def start_webhook_server():
    """Webhookサーバーをバックグラウンドで起動（修正版）"""
    try:
        # ポート競合を避けるため、より高いポート番号を使用
        base_port = int(os.getenv('PORT', 5000))
        port = base_port + 100  # 10000 → 10100
        
        print(f"🌐 Webhookサーバー起動中: ポート {port}")
        webhook_app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
    except Exception as e:
        print(f"❌ Webhookサーバー起動エラー: {e}")

# 起動部分（修正版）
if is_production():
    try:
        webhook_thread = threading.Thread(target=start_webhook_server, daemon=True)
        webhook_thread.start()
        print(f"✅ Webhookサーバー起動完了")
        print(f"🌐 Webhook URL: https://agrilive-order-app.onrender.com/webhook/line")
    except Exception as e:
        print(f"❌ Webhookサーバー起動失敗: {e}")
        print("📝 手動アップロード機能をご利用ください") 