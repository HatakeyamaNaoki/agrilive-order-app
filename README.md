# 受発注データ集計アプリ（アグリライブ）

## 概要
複数の受発注システムからのデータを統合・集計するWebアプリケーションです。手書きPDF注文書もOpenAI APIを使用して自動解析できます。

## 対応フォーマット

### 1. **Infomart形式**
- CSVファイル
- 全角カギカッコ付きヘッダー
- 例: `［伝票No］`, `［発注日］`, `［商品名］`

### 2. **IPORTER形式**
- CSVファイル
- 27行ブロック構造
- 複数商品の明細を含む

### 3. **三菱形式**
- Excelファイル（.xlsx）
- 特定のセル配置
- 伝票番号がB5セルに配置

### 4. **手書きPDF形式** ⭐新機能
- PDFファイル（.pdf）
- OpenAI APIを使用した自動解析
- 手書き文字の認識と構造化

## セットアップ

### 1. 依存関係のインストール
```bash
pip install -r requirements.txt
```

### 2. OpenAI APIキーの設定
```bash
# 環境変数として設定
export OPENAI_API_KEY=your_openai_api_key_here

# または .env ファイルを作成
echo "OPENAI_API_KEY=your_openai_api_key_here" > .env
```

### 3. アプリケーションの起動
```bash
streamlit run app.py
```

## 機能

### ファイルアップロード
- 複数ファイルの同時アップロード対応
- ドラッグ&ドロップ対応
- 対応形式: `.txt`, `.csv`, `.xlsx`, `.pdf`

### データ解析
- 自動フォーマット判定
- エンコーディング自動検出
- 手書きPDFのAI解析

### データ編集
- インライン編集機能
- データの追加・削除・修正
- リアルタイムプレビュー

### 集計・出力
- 商品別集計
- 複数シートのExcel出力
  - 注文一覧
  - 注文一覧（層別結果）
  - 集計結果

## PDF解析機能

### OpenAI API使用
- GPT-4oモデルを使用
- テキストと画像の両方を解析
- 構造化されたJSONデータに変換

### 解析項目
- 伝票番号
- 発注日
- 納品日
- 取引先名
- 商品情報（コード、名前、数量、単位、単価、金額、備考）

### フォールバック機能
- API呼び出し失敗時の簡易解析
- 基本的な情報抽出

## ファイル構成

```
web_app/
├── app.py                    # メインアプリケーション
├── config_loader.py          # 設定読み込み
├── parser_infomart.py        # Infomart解析
├── parser_iporter.py         # IPORTER解析
├── parser_mitsubishi.py      # 三菱解析
├── parser_pdf.py            # PDF解析（新規）
├── config/                   # 設定ファイル
├── requirements.txt          # 依存関係
├── credentials.json          # 認証情報
└── env_example.txt          # 環境変数設定例
```

## 技術スタック

- **フレームワーク**: Streamlit
- **認証**: streamlit-authenticator
- **データ処理**: pandas
- **PDF処理**: pdfplumber, PyPDF2
- **AI解析**: OpenAI API
- **出力**: xlsxwriter

## 注意事項

- OpenAI APIキーが必要です
- PDF解析にはAPIコストが発生します
- 手書き文字の認識精度は文書の品質に依存します