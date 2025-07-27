import openai
import pdfplumber
import io
import json
import os
from PIL import Image
import base64
from enhanced_prompt_pdf import ENHANCED_PDF_ORDER_SYSTEM_PROMPT, STEP_BY_STEP_PROMPT
from image_preprocessing import extract_structured_text, validate_text_quality

def analyze_with_multiple_methods(pdf_bytes, filename):
    """
    複数の方法でPDFを解析し、最良の結果を選択
    """
    results = {}
    
    # 方法1: 従来のテキスト抽出
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text_content = ""
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_content += page_text + "\n"
        results['traditional'] = text_content
    except Exception as e:
        results['traditional'] = f"Error: {e}"
    
    # 方法2: 画像ベースのOCR
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            images = []
            for page_num, page in enumerate(pdf.pages):
                page_images = page.images
                for img in page_images:
                    img_data = img['stream'].get_data()
                    img_base64 = base64.b64encode(img_data).decode('utf-8')
                    images.append({
                        'page': page_num + 1,
                        'data': img_base64,
                        'format': 'png'
                    })
        
        # 画像前処理とOCR
        ocr_results = {}
        for img in images:
            img_bytes = base64.b64decode(img['data'])
            processed_texts = extract_structured_text(img_bytes)
            ocr_results[f"page_{img['page']}"] = processed_texts
        
        results['ocr_enhanced'] = ocr_results
    except Exception as e:
        results['ocr_enhanced'] = f"Error: {e}"
    
    return results

def select_best_result(results):
    """
    複数の解析結果から最良のものを選択
    """
    best_result = None
    best_score = 0
    
    # 従来のテキスト抽出結果を評価
    if 'traditional' in results and isinstance(results['traditional'], str):
        is_valid, quality_info = validate_text_quality(results['traditional'])
        if is_valid:
            # 品質スコアを計算（簡易版）
            japanese_chars = len([c for c in results['traditional'] if '\u4E00' <= c <= '\u9FAF'])
            total_chars = len(results['traditional'].replace(' ', '').replace('\n', ''))
            if total_chars > 0:
                score = japanese_chars / total_chars
                if score > best_score:
                    best_score = score
                    best_result = results['traditional']
    
    # OCR結果を評価
    if 'ocr_enhanced' in results and isinstance(results['ocr_enhanced'], dict):
        for page_key, page_results in results['ocr_enhanced'].items():
            if isinstance(page_results, dict):
                for method, text in page_results.items():
                    if isinstance(text, str):
                        is_valid, quality_info = validate_text_quality(text)
                        if is_valid:
                            japanese_chars = len([c for c in text if '\u4E00' <= c <= '\u9FAF'])
                            total_chars = len(text.replace(' ', '').replace('\n', ''))
                            if total_chars > 0:
                                score = japanese_chars / total_chars
                                if score > best_score:
                                    best_score = score
                                    best_result = text
    
    return best_result, best_score

def analyze_with_confidence(pdf_bytes, filename):
    """
    信頼度付きでPDFを解析
    """
    # 複数方法で解析
    results = analyze_with_multiple_methods(pdf_bytes, filename)
    
    # 最良の結果を選択
    best_text, confidence_score = select_best_result(results)
    
    if not best_text:
        return {
            "order_id": "",
            "order_date": "",
            "delivery_date": "",
            "partner_name": "",
            "items": [],
            "confidence": 0.0,
            "issues": ["テキスト抽出に失敗しました"],
            "suggestions": ["画像品質の確認をお願いします"]
        }
    
    # OpenAI APIで解析
    try:
        from config import get_openai_api_key
        api_key = get_openai_api_key()
        client = openai.OpenAI(api_key=api_key)
        
        # 信頼度情報を含むプロンプト
        enhanced_prompt = f"""
{ENHANCED_PDF_ORDER_SYSTEM_PROMPT}

## 解析対象ファイル
ファイル名: {filename}
信頼度スコア: {confidence_score:.2f}

## 抽出されたテキスト
{best_text}

上記のテキストから注文情報を抽出し、信頼度を考慮して結果を出力してください。
信頼度が低い場合は、代替解釈を必ず提示してください。
"""
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": enhanced_prompt},
                {"role": "user", "content": f"ファイル名: {filename}\n\nテキスト内容:\n{best_text}"}
            ],
            max_tokens=3000,
            temperature=0.1
        )
        
        content = response.choices[0].message.content
        
        # JSONとして解析
        try:
            parsed_data = json.loads(content)
            parsed_data['confidence'] = confidence_score
            return parsed_data
        except json.JSONDecodeError:
            # JSON解析に失敗した場合のフォールバック
            return {
                "order_id": "",
                "order_date": "",
                "delivery_date": "",
                "partner_name": "",
                "items": [],
                "confidence": confidence_score,
                "issues": ["JSON解析に失敗しました"],
                "suggestions": ["手動確認をお願いします"],
                "raw_text": best_text
            }
            
    except Exception as e:
        return {
            "order_id": "",
            "order_date": "",
            "delivery_date": "",
            "partner_name": "",
            "items": [],
            "confidence": confidence_score,
            "issues": [f"API解析エラー: {e}"],
            "suggestions": ["手動確認をお願いします"],
            "raw_text": best_text
        }

def parse_pdf_enhanced(pdf_bytes, filename):
    """
    改善されたPDF解析機能
    """
    try:
        # 信頼度付き解析
        parsed_data = analyze_with_confidence(pdf_bytes, filename)
        
        # 標準形式に変換
        records = []
        
        # 基本情報
        order_id = parsed_data.get("order_info", {}).get("order_id", "")
        order_date = parsed_data.get("order_info", {}).get("order_date", "")
        delivery_date = parsed_data.get("order_info", {}).get("delivery_date", "")
        partner_name = parsed_data.get("order_info", {}).get("partner_name", "")
        confidence = parsed_data.get("order_info", {}).get("confidence", 0.0)
        
        # 商品情報
        items = parsed_data.get("items", [])
        
        for item in items:
            record = {
                "order_id": order_id,
                "order_date": order_date,
                "delivery_date": delivery_date,
                "partner_name": partner_name,
                "product_code": item.get("product_code", ""),
                "product_name": item.get("product_name", ""),
                "quantity": item.get("quantity", ""),
                "unit": item.get("unit", ""),
                "unit_price": item.get("unit_price", ""),
                "amount": item.get("amount", ""),
                "remark": item.get("remark", ""),
                "data_source": filename,
                "confidence": item.get("confidence", confidence),
                "alternatives": item.get("alternatives", [])
            }
            records.append(record)
        
        # 品質評価情報
        quality_info = parsed_data.get("quality_assessment", {})
        if quality_info:
            records.append({
                "order_id": order_id,
                "order_date": order_date,
                "delivery_date": delivery_date,
                "partner_name": partner_name,
                "product_code": "",
                "product_name": "品質評価情報",
                "quantity": "",
                "unit": "",
                "unit_price": "",
                "amount": "",
                "remark": f"信頼度: {quality_info.get('overall_confidence', confidence)}, 問題点: {', '.join(quality_info.get('issues', []))}",
                "data_source": filename,
                "confidence": quality_info.get("overall_confidence", confidence)
            })
        
        return records
        
    except Exception as e:
        print(f"改善版PDF解析エラー: {e}")
        # エラーが発生した場合は空のレコードを返す
        return [{
            "order_id": "",
            "order_date": "",
            "delivery_date": "",
            "partner_name": "",
            "product_code": "",
            "product_name": "解析エラー",
            "quantity": "",
            "unit": "",
            "unit_price": "",
            "amount": "",
            "remark": f"改善版PDF解析エラー: {e}",
            "data_source": filename,
            "confidence": 0.0
        }] 