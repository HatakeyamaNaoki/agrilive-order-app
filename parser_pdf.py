import openai
import pdfplumber
import io
import json
import os
from PIL import Image
import base64
from prompt_pdf import PDF_ORDER_SYSTEM_PROMPT

def extract_text_from_pdf(pdf_bytes):
    """
    PDFからテキストを抽出する
    """
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text = ""
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text
    except Exception as e:
        raise Exception(f"PDFテキスト抽出エラー: {e}")

def extract_images_from_pdf(pdf_bytes):
    """
    PDFから画像を抽出する
    """
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            images = []
            for page_num, page in enumerate(pdf.pages):
                # ページの画像を抽出
                page_images = page.images
                for img in page_images:
                    # 画像データを取得
                    img_data = img['stream'].get_data()
                    # base64エンコード
                    img_base64 = base64.b64encode(img_data).decode('utf-8')
                    images.append({
                        'page': page_num + 1,
                        'data': img_base64,
                        'format': 'png'  # デフォルト形式
                    })
        return images
    except Exception as e:
        raise Exception(f"PDF画像抽出エラー: {e}")

def analyze_handwritten_order_with_openai(pdf_bytes, filename):
    """
    OpenAI APIを使用して手書き注文書を解析する
    """
    # OpenAI APIキーを取得
    from config import get_openai_api_key
    try:
        api_key = get_openai_api_key()
        if not api_key:
            raise Exception("OPENAI_API_KEYが設定されていません")
    except Exception as e:
        # より詳細なエラー情報を提供
        import os
        is_render = os.getenv('RENDER', False)
        if is_render:
            raise Exception(f"本番環境でのAPIキー取得エラー: {e}. Render Secrets Filesの設定を確認してください。")
        else:
            raise Exception(f"ローカル環境でのAPIキー取得エラー: {e}. .envファイルの設定を確認してください。")
    
    client = openai.OpenAI(api_key=api_key)
    
    try:
        # PDFからテキストと画像を抽出
        text_content = extract_text_from_pdf(pdf_bytes)
        images = extract_images_from_pdf(pdf_bytes)
        
        # OpenAI APIに送信するメッセージを構築
        messages = [
            {
                "role": "system",
                "content": PDF_ORDER_SYSTEM_PROMPT
            },
            {
                "role": "user",
                "content": f"以下の注文書を解析してください。ファイル名: {filename}\n\nテキスト内容:\n{text_content}"
            }
        ]
        
        # 画像がある場合は画像も送信
        if images:
            for img in images:
                messages.append({
                    "role": "user",
                    "content": [
                        {"type": "text", "text": f"ページ{img['page']}の画像"},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img['data']}"}}
                    ]
                })
        
        # OpenAI APIを呼び出し
        response = client.chat.completions.create(
            model="gpt-4o",  # または "gpt-4-vision-preview" 画像対応版
            messages=messages,
            max_tokens=2000,
            temperature=0.1
        )
        
        # レスポンスを解析
        content = response.choices[0].message.content
        
        # JSONとして解析
        try:
            # マークダウンの```jsonと```を除去
            cleaned_content = content.strip()
            if cleaned_content.startswith('```json'):
                cleaned_content = cleaned_content[7:]  # ```jsonを除去
            if cleaned_content.endswith('```'):
                cleaned_content = cleaned_content[:-3]  # ```を除去
            cleaned_content = cleaned_content.strip()
            
            parsed_data = json.loads(cleaned_content)
            return parsed_data
        except json.JSONDecodeError as e:
            # JSON解析に失敗した場合、テキストから情報を抽出
            return extract_fallback_data(text_content, filename)
            
    except Exception as e:
        raise Exception(f"OpenAI API解析エラー: {e}")

def extract_fallback_data(text_content, filename):
    """
    OpenAI APIが失敗した場合のフォールバック処理
    """
    # 基本的な情報抽出のロジック
    lines = text_content.split('\n')
    
    # 基本的な情報を抽出（簡易版）
    order_id = ""
    order_date = ""
    delivery_date = ""
    partner_name = ""
    items = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # 伝票番号の抽出
        if "伝票" in line or "注文" in line or "番号" in line:
            import re
            numbers = re.findall(r'\d+', line)
            if numbers:
                order_id = numbers[0]
        
        # 日付の抽出
        import re
        date_pattern = r'(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})'
        dates = re.findall(date_pattern, line)
        if dates and not order_date:
            order_date = f"{dates[0][0]}/{dates[0][1].zfill(2)}/{dates[0][2].zfill(2)}"
        elif dates and not delivery_date:
            delivery_date = f"{dates[0][0]}/{dates[0][1].zfill(2)}/{dates[0][2].zfill(2)}"
    
    return {
        "order_id": order_id,
        "order_date": order_date,
        "delivery_date": delivery_date,
        "partner_name": partner_name,
        "items": items
    }

def parse_pdf_handwritten(pdf_bytes, filename):
    """
    PDFの手書き注文書を解析して標準形式に変換
    """
    try:
        # OpenAI APIで解析
        parsed_data = analyze_handwritten_order_with_openai(pdf_bytes, filename)
        
        # 標準形式に変換
        records = []
        order_id = parsed_data.get("order_id", "")
        order_date = parsed_data.get("order_date", "")
        delivery_date = parsed_data.get("delivery_date", "")
        partner_name = parsed_data.get("partner_name", "")
        items = parsed_data.get("items", [])
        
        # 日付形式の変換（MM/DD、〇月〇日 → YYYY/MM/DD）
        def convert_date_format(date_str):
            if not date_str:
                return ""
            
            import re
            from datetime import datetime
            
            # MM/DD形式の場合、現在の年を追加
            mmdd_pattern = r'^(\d{1,2})/(\d{1,2})$'
            match = re.match(mmdd_pattern, date_str)
            if match:
                month = match.group(1).zfill(2)
                day = match.group(2).zfill(2)
                current_year = datetime.now().year
                return f"{current_year}/{month}/{day}"
            
            # 〇月〇日形式の場合、現在の年を追加
            japanese_date_pattern = r'^(\d{1,2})月(\d{1,2})日$'
            match = re.match(japanese_date_pattern, date_str)
            if match:
                month = match.group(1).zfill(2)
                day = match.group(2).zfill(2)
                current_year = datetime.now().year
                return f"{current_year}/{month}/{day}"
            
            return date_str
        
        # 日付を変換
        order_date = convert_date_format(order_date)
        delivery_date = convert_date_format(delivery_date)
        
        # 各商品を標準形式に変換
        for item in items:
            # キー名が違う場合はここで修正
            product_name = item.get("product_name") or item.get("商品名") or ""
            quantity = item.get("quantity") or item.get("数量") or ""
            if not product_name or not quantity:
                continue
            record = {
                "order_id": order_id,
                "order_date": order_date,
                "delivery_date": delivery_date,
                "partner_name": partner_name,
                "product_code": item.get("product_code", "") or item.get("商品コード", ""),
                "product_name": product_name,
                "quantity": quantity,
                "unit": item.get("unit", "") or item.get("単位", ""),
                "unit_price": item.get("unit_price", "") or item.get("単価", ""),
                "amount": item.get("amount", "") or item.get("金額", ""),
                "remark": item.get("remark", "") or item.get("備考", ""),
                "data_source": filename
            }
            records.append(record)
        
        # itemsが空の場合、基本的な情報のみのレコードを作成
        if not items:
            record = {
                "order_id": order_id,
                "order_date": order_date,
                "delivery_date": delivery_date,
                "partner_name": partner_name,
                "product_code": "",
                "product_name": "商品情報なし",
                "quantity": "",
                "unit": "",
                "unit_price": "",
                "amount": "",
                "remark": "PDF解析で商品情報を抽出できませんでした",
                "data_source": filename
            }
            records.append(record)
        
        return records
        
    except Exception as e:
        raise Exception(f"PDF解析エラー: {e}") 