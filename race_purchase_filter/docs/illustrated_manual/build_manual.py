"""アプリの実装に基づき、図解入りPDFと当日確認記録を再生成する。"""
from pathlib import Path
import csv
import json
import hashlib
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.colors import HexColor, white
from reportlab.lib.utils import ImageReader
from pypdf import PdfReader

# 保存先は説明書用フォルダに限定し、アプリの検証データには書き込まない。
OUT = Path(__file__).resolve().parent
APP = OUT.parent.parent
PROJECT = APP.parent
REVIEW = PROJECT / 'races_review/2026/202611'
rows = list(csv.DictReader((REVIEW / 'race_review_20261121.csv').open(encoding='utf-8-sig')))
horses = list(csv.DictReader((REVIEW / 'horse_review_20261121.csv').open(encoding='utf-8-sig')))
pdfmetrics.registerFont(TTFont('JP', 'C:/Windows/Fonts/meiryo.ttc', subfontIndex=0))
pdfmetrics.registerFont(TTFont('JPB', 'C:/Windows/Fonts/meiryob.ttc', subfontIndex=0))
W, H = 595.28, 841.89
NAVY, TEAL, ORANGE = '#132E49', '#087F82', '#CF7613'
GRAY, PAPER = '#526170', '#F5F8FA'
c = canvas.Canvas(str(OUT / '操作説明書_イラスト付き.pdf'), pagesize=(W, H))
c.setTitle('レース購入適性判定 操作説明書・2026年11月21日確認結果')
c.setAuthor('race_purchase_filter 操作ガイド')


def box(x, y, w, h, fill=PAPER, stroke=None, radius=9):
    # 上端を基準に描画し、各ページの余白を統一する。
    c.setFillColor(HexColor(fill))
    c.setStrokeColor(HexColor(stroke or fill))
    c.roundRect(x, H-y-h, w, h, radius, fill=1, stroke=bool(stroke))


def txt(x, y, text, size=10, color=NAVY, bold=False):
    c.setFillColor(HexColor(color))
    c.setFont('JPB' if bold else 'JP', size)
    c.drawString(x, H-y-size, str(text))


def lines(x, y, texts, size=10, gap=18, color=NAVY):
    for i, text in enumerate(texts):
        txt(x, y+i*gap, text, size, color)


def base(num, title, subtitle):
    c.setFillColor(white)
    c.rect(0, 0, W, H, fill=1, stroke=0)
    txt(38, 24, 'RACE PURCHASE FILTER  /  操作説明書', 9, TEAL, True)
    txt(38, 45, title, 23, bold=True)
    txt(38, 82, subtitle, 9, GRAY)
    c.setStrokeColor(HexColor('#DCE5EB'))
    c.line(38, 42, W-38, 42)
    txt(38, H-31, '実装確認日 2026-11-21  |  検証用の暫定判定・自動投票機能なし', 8, GRAY)
    txt(W-65, H-31, f'{num} / 6', 8, GRAY)


def heading(y, title):
    txt(38, y, title, 13, TEAL, True)


def note(y, title, body, height=75):
    box(38, y, W-76, height, '#FFF3E0')
    txt(51, y+11, title, 11, ORANGE, True)
    lines(51, y+32, body, 9, 16)


def screen(y, active, fields, buttons):
    # 実画面のラベルを使った説明用模式図。スクリーンショットではない。
    x, width = 38, W-76
    height = 85 + len(fields)*25 + len(buttons)*25
    box(x, y, width, height, '#F6F7F9', '#C4D0DA')
    box(x+1, y+1, width-2, 27, '#DCEAF4', radius=8)
    txt(x+13, y+6, '独立レース購入適性判定', 10, bold=True)
    tabs = ['事前判定入力', 'レース一覧', 'レース後結果', '方式比較']
    for i, tab in enumerate(tabs):
        box(x+10+i*126, y+34, 121, 25, TEAL if tab == active else '#E3E8EE', radius=4)
        txt(x+19+i*126, y+40, tab, 9, '#FFFFFF' if tab == active else NAVY)
    for i, (label, value) in enumerate(fields):
        top = y+72+i*25
        txt(x+14, top+3, label, 9)
        box(x+265, top, width-280, 21, '#FFFFFF', '#C8D2DB', 3)
        txt(x+273, top+3, value, 8.5)
    top = y+75+len(fields)*25
    for i, button in enumerate(buttons):
        box(x+14, top+i*25, width-28, 21, '#DCEFED', radius=4)
        txt(x+24, top+3+i*25, button, 9, TEAL, True)
    return y+height


def table(y, headers, values, widths, size=9, rowh=27):
    x = 38
    box(x, y, sum(widths), rowh, NAVY, radius=3)
    for j, value in enumerate(headers):
        txt(x+sum(widths[:j])+6, y+7, value, size, '#FFFFFF', True)
    for i, row in enumerate(values):
        top = y+rowh*(i+1)
        box(x, top, sum(widths), rowh, '#F0F5F8' if i%2 == 0 else '#FFFFFF', radius=0)
        for j, value in enumerate(row):
            cellw = widths[j]-10
            actual_size = min(size, cellw/max(pdfmetrics.stringWidth(str(value), 'JP', size), 1)*size)
            txt(x+sum(widths[:j])+6, top+7, value, actual_size)


# 1ページ：生成したイラストを、文字の読みやすい大きさで配置する。
base(1, 'ひと目で分かる操作の流れ', '最初に全体像を確認し、次のページから実際の入力手順へ進みます。')
im = ImageReader(str(OUT/'操作ガイド.png'))
iw, ih = im.getSize()
width = W-76
height = width*ih/iw
c.drawImage(im, 38, H-112-height, width=width, height=height)
lines(38, 701, ['図中の画面・グラフは操作を説明する概念図です。実績値を表していません。',
                '実際のボタン名・入力例・集計上の制約は、2〜4ページを参照してください。',
                '5〜6ページには、2026年11月21日の保存記録の確認結果を掲載しています。'], 9, 20)
c.showPage()

# 2ページ：事前入力の順序と、入力が自動更新されない項目を明記する。
base(2, 'レース前：選ぶ → 入力 → 保存', '評価不一致数は自動算出されません。事前予想に基づいて手動で入力します。')
heading(112, '1  起動して、対象のレースを選ぶ')
lines(38, 139, ['① アプリフォルダの run.bat をダブルクリック。Python・tkinter が必要です。',
                '②「事前判定入力」→「xlsx 読込」で対象日のExcelを選びます。',
                '③「レース一覧」→ 競馬場ボタン → 対象レースをダブルクリック。',
                '   または「事前判定入力へ送る」を押すと race_id とレース名が反映されます。'], 9.5, 19)
screen(228, '事前判定入力', [
    ('race_id', '例：202608050511'),
    ('モデル世代（手動入力）', '使用した世代を入力'),
    ('評価不一致馬の数（手動入力のみ）', '例：1'),
    ('評価不一致 該当馬番号', '例：7'),
    ('予想TOP5馬番（任意）', '例：3,5,9,11,13'),
    ('レース名・クラス', '選択したレース名'),
    ('比較困難の頭数・馬番・根拠メモ', '分かる範囲を記録'),
], ['判定を表示', '事前判定スナップショットを保存（追記専用・変更不可）'])
heading(559, '2  入力内容を確認し、発走前に保存する')
lines(38, 586, ['不一致0〜1頭 → 購入候補 ／ 2頭以上 → 見送り候補 ／ 空欄 → 判定保留。',
                '比較困難データがなければ「データ未取得」をチェックします。',
                '「新馬・未勝利戦を購入対象外とする」と条件バージョンも確認します。'], 9.5, 19)
note(659, 'レースを切り替えるたびに、前の入力を確認', [
    'レース一覧から反映されるのは race_id とレース名だけです。他欄は残ります。',
    '保存前にモデル世代・不一致数・馬番・TOP5を対象レースの内容に直します。',
    '保存後は変更・削除不可。同一 race_id ＋同一原本SHA256の再保存はエラーです。'], 101)
c.showPage()

# 3ページ：実際に入力する金額と、練習例と実績の区別を明確にする。
base(3, 'レース後：着順と購入券を記録', '「レース後結果」タブで、確定情報と実際の購入内容を入力します。')
screen(116, 'レース後結果', [
    ('race_id', '事前記録と同じID'),
    ('確定1〜3着馬番（着順どおり）', '3,5,7'),
    ('予想TOP5馬番（任意）', '3,5,9,11,13'),
    ('券種', '馬連'),
    ('組み合わせ', '3-5'),
    ('金額（円）', '1000'),
    ('払戻額（円）', '2500'),
], ['結果を保存（results.jsonl へ追記）', '結果CSV取込 ／ チケットCSV取込'])
heading(446, '入力例の読み方（上図は練習例・今日の実績ではありません）')
lines(38, 474, ['金額はその買い目の購入額、払戻額はその購入券で実際に受け取る総額です。',
                '上図は的中。損益は 2,500 − 1,000 = ＋1,500円、回収率は250.0％です。',
                '確定1〜3着のうち予想TOP5に含まれるのは2頭なので、捕捉率は2/3です。',
                '金額は「1000」のように数字だけで入力。確定した不的中の払戻は「0」。',
                '払戻が不明なら空欄にします。空欄は未計算になり、0円とは扱われません。'], 9.5, 20)
note(591, '保存・CSV取込の注意', [
    '結果入力欄の race_id は自動転記されません。事前記録と同じIDを手で入力します。',
    '保存ボタンの連打・同じCSVの再取込は避けます。結果には重複防止がありません。',
    '複数券・結果CSVと券CSVの併用には、4ページの集計上の制約があります。'], 96)
lines(38, 711, ['CSV列仕様は同梱の sample_results.csv / sample_tickets.csv を参照。',
                'サンプルCSVは練習データです。本番の data/ へ取り込まないでください。',
                'TOP5は事前記録から集計時に補完できます。券CSV単独なら着順・TOP5も含めます。'], 8.6, 18)
c.showPage()

# 4ページ：READMEだけでなく、集計関数を読んで分かった制約も説明する。
base(4, '方式比較の読み方と現行の制約', '「方式比較」→「再計算」。比較条件は同一原本・同一券種・同一金額にそろえます。')
table(119, ['方式', '集計対象・表示内容'], [
    ('A', '購入券を記録した全体（事前に対象外指定したレースは除外）'),
    ('B', 'Aのうち、事前記録の評価不一致数が0〜1頭のもの'),
    ('C', '閾値未確定。常に未算出。利益・回収率は計算しない'),
], [45, W-121], 9, 34)
heading(282, '見る数値')
lines(38, 309, ['損益 ＝ 払戻額 − 投資額。回収率 ＝ 払戻額 ÷ 投資額 ×100。',
                'TOP5捕捉率 ＝ 捕捉頭数の合計 ÷（記録された対象件数 ×3）×100。',
                '投資0円や金額不明では回収率は未計算。不一致不明はBから除外されます。',
                '画面は保存済み全期間の合算です。日付・モデル世代で絞る機能はありません。'], 9.5, 20)
note(410, '重要：表示の「購入レース数」は、現行実装では購入券付き記録数', [
    '同じレースを買い目ごとに別保存すると、別レースとして数えられます。',
    '投資・払戻は各記録から合算しますが、レース数やTOP5率が偏ることがあります。',
    '複数券の比較を読む際は、race_id ごとの件数も別途確認してください。'], 96)
note(522, '結果CSVとチケットCSVは、現行集計では完全に結合されません', [
    '結果CSVは券なし記録、チケットCSVは券付き記録として個別に追記されます。',
    '着順だけの結果記録と券だけの記録を取り込んでも、相互に着順を補完しません。',
    '券CSVには finish_top3 と predicted_top5 も含めると確認しやすくなります。'], 96)
lines(38, 643, ['そのほかの注意',
                '・枠連：画面には選択肢がありますが、実装は馬番で照合しています。',
                '  正しい枠番照合に未対応なので、枠連の的中表示を実績判定に使わないでください。',
                '・複勝：一律1〜3着で判定します。少頭数などの発売条件は別途確認が必要です。',
                '・再起動時：保存済み判定・結果の集計は復元されます。',
                '  レース一覧はExcelを再読込します。終了は右上の×。バックアップは data/ をコピー。'], 8.7, 18)
c.showPage()

# 5ページ：保存データと公開結果を区別し、未記録を損失ゼロと読み違えないようにする。
base(5, '今日の確認結果：収支は評価不能', '対象日：2026年11月21日（土）／ 日本時間。保存記録を変更せずに確認しました。')
table(120, ['確認対象', '確認した内容'], [
    ('アプリの事前記録', 'data/snapshots.jsonl は存在せず、保存記録0件'),
    ('アプリの結果・購入券', 'data/results.jsonl は存在せず、保存記録0件'),
    ('当日の回顧データ', f'{len(rows)}レース（京都12・福島12）、馬別記録{len(horses)}行'),
    ('当日の事前予想', '回顧CSVの全24行で prediction_top1 が空欄'),
    ('A方式・B方式', '今日の購入額・払戻・回収率・見送り効果は評価不能'),
    ('C方式', '条件未確定のため未算出'),
], [133, W-209], 9, 34)
lines(38, 375, ['回顧CSVは全24行で「当日予想未生成（運用処理未完了）」と記録しています。',
                'このアプリでの購入記録がないことは、実際に馬券を買わなかった証拠ではありません。',
                '結果を見た後で不一致数や予想を補っても、前向き検証の実績にはできません。'], 9, 19)
heading(451, '主要レースの確定着順（公開結果と照合済み）')
table(480, ['レース', '1着 → 2着 → 3着'], [
    ('福島11R 福島記念', '13 マリアイリダータ → 3 ゼンダンハヤブサ → 1 カラマティアノス'),
    ('京都11R シトリンS', '12 メルカントゥール → 10 テーオーマルコーニ → 9 レッドプロフェシー'),
], [127, W-203], 8, 36)
lines(38, 602, ['確認元（詳細なローカル根拠とURLは「今日の結果確認_20261121.txt」に記載）',
                '・JRA開催日程：京都・福島の開催予定を確認。',
                '・netkeiba：上記2レースの1〜3着と、ローカル回顧CSVの一致を確認。',
                '・全24Rの次ページ一覧は保存済み回顧CSVの転記。全件の再照合は未実施。'], 8.7, 19)
note(704, '次回の検証で必要なこと', ['発走前に事前判定を保存し、レース後に実際の買い目・購入額・払戻を記録します。',
    'Pythonが見つからず起動しない場合は、Python環境とPATH設定を確認してください。'], 70)
c.showPage()

# 6ページ：各場の全レースを、元CSVの着順のまま掲載する。
base(6, '今日の全24レース：保存済み着順', '出典：race_review_20261121.csv。主要2レース以外はオンラインでの再照合を行っていません。')
for place, top in [('福島', 112), ('京都', 449)]:
    heading(top, f'{place}  1〜12R')
    vals = [(r['race_no'], r['race_name'], r['winner'], r['second'], r['third'])
            for r in rows if r['racecourse'] == place]
    table(top+26, ['R', 'レース名', '1着', '2着', '3着'], vals,
          [22, 109, 129, 129, W-465], 7.5, 22)
c.showPage()
c.save()

# 今日の確認の根拠を、機械可読・テキストの両方で残す。
sources = [APP/'app.py', APP/'core.py', APP/'storage.py', APP/'excel_loader.py', APP/'README.md',
           REVIEW/'race_review_20261121.csv', REVIEW/'horse_review_20261121.csv', REVIEW/'model_review_20261121.csv']
evidence = {
    'checked_at_jst': datetime.now(ZoneInfo('Asia/Tokyo')).isoformat(),
    'target_date': '2026-11-21',
    'snapshot_file_exists': (APP/'data/snapshots.jsonl').exists(),
    'result_file_exists': (APP/'data/results.jsonl').exists(),
    'race_count': len(rows), 'horse_rows': len(horses),
    'place_counts': dict(Counter(r['racecourse'] for r in rows)),
    'missing_prediction_top1': sum(not r['prediction_top1'] for r in rows),
    'prediction_status_counts': dict(Counter(r['prediction_error_type'] for r in rows)),
    'sources': [{'path': str(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in sources],
    'online_checks': [
        'https://www.jra.go.jp/keiba/calendar2026/2026/11/1121.html',
        'https://race.netkeiba.com/race/result.html?race_id=202603030511',
        'https://race.netkeiba.com/race/result.html?race_id=202608050511'],
    'online_scope': 'JRA開催予定と主要2レースの上位3頭を照合。全24レースの再照合ではない。',
    'image_generation': '組み込みimage_genツール。概念図と実画面は異なる。',
}
(OUT/'確認根拠_20261121.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding='utf-8')
summary = ['2026年11月21日 結果確認', '',
    '結論：race_purchase_filter の保存記録は0件。今日の収支・A/B比較の効果は評価不能。',
    'data/snapshots.jsonl と data/results.jsonl はどちらも存在しない。data/には .gitkeep のみ。',
    '未記録は実購入ゼロや損益ゼロを意味しない。',
    f'別フォルダの回顧CSV：{len(rows)}レース（京都12R・福島12R）、馬別記録{len(horses)}行。',
    '全24行で prediction_top1 は空欄。prediction_error_type は当日予想未生成（運用処理未完了）。',
    '不一致数・事前TOP5・購入券・金額が不足しており、前向き検証の再現はできない。', '',
    '公開結果と照合した主要2レース：',
    '福島11R 福島記念：13マリアイリダータ、3ゼンダンハヤブサ、1カラマティアノス。',
    '京都11R シトリンS：12メルカントゥール、10テーオーマルコーニ、9レッドプロフェシー。',
    '全24RはPDFの6ページ参照。主要2レース以外はローカルCSVの転記で、オンライン再照合は未実施。', '',
    '確認元ファイル：', *[str(p) for p in sources], '', '公開確認元：', *evidence['online_checks'], '',
    '操作説明書の注意：',
    '画面図は実装ラベルを用いた模式図。実画面キャプチャではない。',
    '購入レース数は券付き結果レコード単位。複数券で同じrace_idを保存すると重複計数される。',
    '結果CSVと券CSVの着順は集計時に相互補完されない。',
    '枠連は馬番で照合する実装のため、枠連の的中実績判定に使わない。',
    '既存アプリ・元データの編集や、結果の取り込みは行っていない。',
    'run.bat は python app.py を使用する。今回のシェルでは python コマンドが見つからなかった。',
    '説明書の作成にはCodexの同梱Pythonを使用した。', '',
    'イラスト生成：組み込みimage_gen。最終プロンプトの要旨：',
    '日本語の6コマ操作ガイド。起動→Excel読込→レース選択→手動不一致数と事前保存→結果入力→A/B/C比較。',
    '馬のマスコット、フォルダ・マウス・馬券・グラフで図解。紺・緑・橙の配色。',
    '不一致0〜1頭は購入候補、2頭以上は見送り候補、不明は保留。C未算出。自動投票なし、事前記録変更不可。',
    '実績値は記載せず、画面は概念図と明記。',
]
(OUT/'今日の結果確認_20261121.txt').write_text('\n'.join(summary), encoding='utf-8-sig')
reader = PdfReader(str(OUT/'操作説明書_イラスト付き.pdf'))
assert len(reader.pages) == 6
for phrase in ['評価不能', '全24レース', '枠連']:
    assert phrase in ''.join(p.extract_text() for p in reader.pages), phrase
print(json.dumps({'pdf_pages':len(reader.pages), 'race_count':len(rows), 'horse_rows':len(horses),
                  'output':str(OUT)}, ensure_ascii=False))
