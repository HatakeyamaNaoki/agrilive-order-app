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
    信頼度付きでPDFを解析（左右2段構成対応版）
    """
    # 複数方法で解析
    results = analyze_with_multiple_methods(pdf_bytes, filename)
    
    # 最良の結果を選択
    best_text, confidence_score = select_best_result(results)
    
    if not best_text:
        return {
            "document_structure": {
                "layout_type": "不明",
                "detection_confidence": 0.0,
                "layout_notes": "テキスト抽出に失敗"
            },
            "order_info": {
                "order_id": "",
                "order_date": "",
                "delivery_date": "",
                "partner_name": "",
                "confidence": 0.0
            },
            "items": [],
            "quality_assessment": {
                "overall_confidence": 0.0,
                "layout_issues": ["テキスト抽出に失敗しました"],
                "issues": ["テキスト抽出に失敗しました"],
                "suggestions": ["画像品質の確認をお願いします"]
            }
        }
    
    # OpenAI APIで解析
    try:
        from config import get_openai_api_key
        api_key = get_openai_api_key()
        client = openai.OpenAI(api_key=api_key)
        
        # 左右2段構成対応の信頼度情報を含むプロンプト
        enhanced_prompt = f"""
{ENHANCED_PDF_ORDER_SYSTEM_PROMPT}

## 解析対象ファイル
ファイル名: {filename}
信頼度スコア: {confidence_score:.2f}

## 重要: レイアウト検知の優先順位
1. **最初に左右2段構成かどうかを判断**: 商品名が左右2段に配置されているかを確認
2. **別商品として扱う**: 同じ行の左段と右段は別の商品として扱い、別々の行で出力する
3. **備考欄の確認**: 右段の商品名が左段の商品の備考に記載されることが多い
4. **既印字パターンの検知**: 商品名が既に印字されており、数量のみ手書きされているかを確認
5. **数量空白の処理**: 既印字パターンでは数量が空白でも備考欄に記載されることがある
6. **段階的処理**: レイアウトを最初に判断してから文字読み取りを実施

## 抽出されたテキスト
{best_text}

上記のテキストから注文情報を抽出し、左右2段構成や既印字パターンを適切に検知してください。
同じ行の左段と右段は別の商品として扱い、別々の行で出力してください。
信頼度が低い場合は、代替解釈を必ず提示してください。
"""
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": enhanced_prompt},
                {"role": "user", "content": f"ファイル名: {filename}\n\nテキスト内容:\n{best_text}"}
            ],
            max_tokens=4000,
            temperature=0.1
        )
        
        content = response.choices[0].message.content
        
        # JSONとして解析
        try:
            parsed_data = json.loads(content)
            # 信頼度情報を統合
            if "order_info" in parsed_data:
                parsed_data["order_info"]["confidence"] = confidence_score
            if "quality_assessment" in parsed_data:
                parsed_data["quality_assessment"]["overall_confidence"] = confidence_score
            return parsed_data
        except json.JSONDecodeError:
            # JSON解析に失敗した場合のフォールバック
            return {
                "document_structure": {
                    "layout_type": "不明",
                    "detection_confidence": confidence_score,
                    "layout_notes": "JSON解析に失敗"
                },
                "order_info": {
                    "order_id": "",
                    "order_date": "",
                    "delivery_date": "",
                    "partner_name": "",
                    "confidence": confidence_score
                },
                "items": [],
                "quality_assessment": {
                    "overall_confidence": confidence_score,
                    "layout_issues": ["JSON解析に失敗しました"],
                    "issues": ["JSON解析に失敗しました"],
                    "suggestions": ["手動確認をお願いします"]
                },
                "raw_text": best_text
            }
            
    except Exception as e:
        return {
            "document_structure": {
                "layout_type": "不明",
                "detection_confidence": confidence_score,
                "layout_notes": f"API解析エラー: {e}"
            },
            "order_info": {
                "order_id": "",
                "order_date": "",
                "delivery_date": "",
                "partner_name": "",
                "confidence": confidence_score
            },
            "items": [],
            "quality_assessment": {
                "overall_confidence": confidence_score,
                "layout_issues": [f"API解析エラー: {e}"],
                "issues": [f"API解析エラー: {e}"],
                "suggestions": ["手動確認をお願いします"]
            },
            "raw_text": best_text
        }

def parse_pdf_enhanced(pdf_bytes, filename):
    """
    改善されたPDF解析機能（左右2段構成対応版）
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
        
        # レイアウト情報
        layout_info = parsed_data.get("document_structure", {})
        layout_type = layout_info.get("layout_type", "不明")
        layout_confidence = layout_info.get("detection_confidence", 0.0)
        layout_notes = layout_info.get("layout_notes", "")
        
        # 商品情報
        items = parsed_data.get("items", [])
        
        for item in items:
            # 左右2段構成の場合の商品名処理
            product_name = item.get("product_name", "")
            is_preprinted = item.get("is_preprinted", False)
            
            # 既印字商品名の場合は備考に記載
            remark = item.get("remark", "")
            if is_preprinted:
                if remark:
                    remark += " | 既印字商品名"
                else:
                    remark = "既印字商品名"
            
            record = {
                "order_id": order_id,
                "order_date": order_date,
                "delivery_date": delivery_date,
                "partner_name": partner_name,
                "product_code": item.get("product_code", ""),
                "product_name": product_name,
                "quantity": item.get("quantity", ""),
                "unit": item.get("unit", ""),
                "unit_price": item.get("unit_price", ""),
                "amount": item.get("amount", ""),
                "remark": remark,
                "data_source": filename,
                "confidence": item.get("confidence", confidence),
                "alternatives": item.get("alternatives", []),
                "layout_type": layout_type,
                "layout_confidence": layout_confidence,
                "is_preprinted": is_preprinted
            }
            records.append(record)
        
        # レイアウト情報を別レコードとして追加
        if layout_type != "不明":
            records.append({
                "order_id": order_id,
                "order_date": order_date,
                "delivery_date": delivery_date,
                "partner_name": partner_name,
                "product_code": "",
                "product_name": "レイアウト情報",
                "quantity": "",
                "unit": "",
                "unit_price": "",
                "amount": "",
                "remark": f"レイアウトタイプ: {layout_type}, 信頼度: {layout_confidence:.2f}, 備考: {layout_notes}",
                "data_source": filename,
                "confidence": layout_confidence,
                "layout_type": layout_type,
                "layout_confidence": layout_confidence,
                "is_preprinted": False
            })
        
        # 品質評価情報
        quality_info = parsed_data.get("quality_assessment", {})
        if quality_info:
            layout_issues = quality_info.get("layout_issues", [])
            issues = quality_info.get("issues", [])
            suggestions = quality_info.get("suggestions", [])
            
            all_issues = layout_issues + issues
            issue_text = ", ".join(all_issues) if all_issues else "なし"
            suggestion_text = ", ".join(suggestions) if suggestions else "なし"
            
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
                "remark": f"信頼度: {quality_info.get('overall_confidence', confidence)}, 問題点: {issue_text}, 改善提案: {suggestion_text}",
                "data_source": filename,
                "confidence": quality_info.get("overall_confidence", confidence),
                "layout_type": layout_type,
                "layout_confidence": layout_confidence,
                "is_preprinted": False
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
            "confidence": 0.0,
            "layout_type": "不明",
            "layout_confidence": 0.0,
            "is_preprinted": False
        }] 