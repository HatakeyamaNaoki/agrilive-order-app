import cv2
import numpy as np
from PIL import Image
import pytesseract
from skimage import filters, morphology
import io

def enhance_image_for_ocr(image_bytes):
    """
    OCR精度向上のための画像前処理
    """
    # バイトデータをOpenCV形式に変換
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    
    # 1. ノイズ除去
    denoised = cv2.fastNlMeansDenoisingColored(img, None, 10, 10, 7, 21)
    
    # 2. コントラスト強調
    lab = cv2.cvtColor(denoised, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8,8))
    cl = clahe.apply(l)
    enhanced = cv2.merge((cl,a,b))
    enhanced = cv2.cvtColor(enhanced, cv2.COLOR_LAB2BGR)
    
    # 3. 二値化（複数手法を試行）
    gray = cv2.cvtColor(enhanced, cv2.COLOR_BGR2GRAY)
    
    # 適応的二値化
    binary_adaptive = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
    )
    
    # Otsu's二値化
    _, binary_otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    
    # 4. モルフォロジー処理でノイズ除去
    kernel = np.ones((1,1), np.uint8)
    binary_adaptive = cv2.morphologyEx(binary_adaptive, cv2.MORPH_CLOSE, kernel)
    binary_otsu = cv2.morphologyEx(binary_otsu, cv2.MORPH_CLOSE, kernel)
    
    return {
        'original': img,
        'enhanced': enhanced,
        'binary_adaptive': binary_adaptive,
        'binary_otsu': binary_otsu
    }

def detect_text_regions(image):
    """
    テキスト領域の検出
    """
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    
    # MSER（Maximally Stable Extremal Regions）でテキスト領域を検出
    mser = cv2.MSER_create()
    regions, _ = mser.detectRegions(gray)
    
    # 領域を矩形で囲む
    boxes = []
    for region in regions:
        x, y, w, h = cv2.boundingRect(region)
        if w > 20 and h > 10:  # 小さすぎる領域を除外
            boxes.append((x, y, w, h))
    
    return boxes

def extract_structured_text(image_bytes):
    """
    構造化されたテキスト抽出
    """
    try:
        processed_images = enhance_image_for_ocr(image_bytes)
        
        results = {}
        
        # 複数の前処理結果でOCRを実行
        for method, img in processed_images.items():
            if method == 'original':
                continue
                
            # PIL形式に変換
            pil_img = Image.fromarray(img)
            
            # OCR実行（日本語設定）
            try:
                text = pytesseract.image_to_string(
                    pil_img, 
                    lang='jpn',
                    config='--psm 6 --oem 3'
                )
                results[method] = text.strip()
            except Exception as e:
                results[method] = f"OCR Error: {e}"
        
        return results
    except Exception as e:
        print(f"画像前処理エラー: {e}")
        return {"error": f"画像前処理エラー: {e}"}

def validate_text_quality(text):
    """
    テキスト品質の検証
    """
    try:
        if not text:
            return False, "テキストが空です"
        
        # 日本語文字の割合
        japanese_chars = len([c for c in text if '\u3040' <= c <= '\u309F' or '\u30A0' <= c <= '\u30FF' or '\u4E00' <= c <= '\u9FAF'])
        total_chars = len(text.replace(' ', '').replace('\n', ''))
        
        if total_chars == 0:
            return False, "有効な文字がありません"
        
        japanese_ratio = japanese_chars / total_chars
        
        # 数字の割合
        digits = len([c for c in text if c.isdigit()])
        digit_ratio = digits / total_chars if total_chars > 0 else 0
        
        quality_score = (japanese_ratio * 0.6) + (digit_ratio * 0.4)
        
        return quality_score > 0.3, f"品質スコア: {quality_score:.2f} (日本語: {japanese_ratio:.2f}, 数字: {digit_ratio:.2f})"
    except Exception as e:
        print(f"テキスト品質検証エラー: {e}")
        return False, f"品質検証エラー: {e}" 