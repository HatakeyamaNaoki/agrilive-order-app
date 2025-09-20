def get_taxonomy_classification_prompt():
    """
    商品分類用のプロンプトを返す
    """
    return """
あなたは日本の青果物・花の分類器です。
出力は必ずJSONのみ（説明文やコードブロックなし）。
対象は商品名欄の文字列ですが、非商品（例：小計、合計、注意書き、日付だけ、サイズだけ、JANだけ 等）も混在します。
各入力ごとに、以下の項目を返してください：
- is_product: true/false（明確に非商品と判定できるときだけ false。それ以外は true。分類に自信がなくても true）
- major: 野菜/果物/花/その他（分類に自信がない/当てはまらない場合は 'その他'）
- sub: 次から1つ：
  * 野菜: 根もの, 葉もの, 果菜類（実を食べる野菜）, きのこ類, 香味野菜, その他
  * 果物: 柑橘類, 核果類, 仁果類（りんご・なし系）, バナナ・熱帯果実, ぶどう類, いちご類, その他
  * 花: 切花, 鉢花, 花木, 葉物・グリーン, その他
  * major='その他' のときは sub も 'その他'
- canonical: 一般的な商品名（産地・等級・サイズ等は除去）
- yomi: canonical のひらがな読み
必須制約：入力配列と同じ件数・順番の配列で返す。
"""

def get_taxonomy_user_prompt(product_inputs):
    """
    商品分類用のユーザープロンプトを生成する
    """
    import json
    return "以下を分類してください（配列で返す）：\n" + json.dumps(
        [{"input": product_input} for product_input in product_inputs], 
        ensure_ascii=False
    )

