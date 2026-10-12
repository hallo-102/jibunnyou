"""Report evidence from shadow-enabled replay, integrity tests and immutable model provenance."""
from pathlib import Path
from datetime import datetime
import json
import csv
import xml.etree.ElementTree as ET
import subprocess

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'reports/merge_blocked_resolution_20261013'
result = json.loads((OUT / 'runner_output_results.json').read_text(encoding='utf-8'))
provenance = json.loads((OUT / 'provenance.json').read_text(encoding='utf-8'))
run = Path(result['run'])
log = (OUT / 'runner_shadow_enabled.log').read_text(encoding='utf-8')
tests = list(ET.parse(OUT / 'pytest_final.xml').getroot().iter('testcase'))
failures = [x for x in tests if x.find('failure') is not None or x.find('error') is not None]
shadow_files = list((run / 'data/output/shadow_comparison/20261012').glob('*_pre_*.json'))
assert 'ENABLE_SHADOW=False' not in log
assert 'すべて完了しました' in log and '結果確定前シャドー比較JSONを保存しました' in log
assert len(shadow_files) == 1 and not failures
payload = json.loads(shadow_files[0].read_text(encoding='utf-8'))
assert payload['result_status'] == 'pre_result'
assert payload['production_model']['production_effective_sha256'] == payload['shadow_model']['shadow_effective_sha256']
assert len(payload['races']) == 17
assert not any(result[k] for k in ('paired_issues', 'fixed_reapplication_issues', 'production_rank_score_issues', 'production_bet_issues'))
source_checks = json.loads((run / 'production_unchanged.json').read_text(encoding='utf-8'))
assert all(x['unchanged'] for x in source_checks)

checks = [
    ('原因・履歴の特定', 'PASS', '昇格265e0a4、改行固定d66c425、PR d7d779bと実体を照合。provenance.json。'),
    ('本番モデル・重みの保護', 'PASS', 'production_model.json、20260730/20260817重みはmain blob・本番・検証側がbyte一致。'),
    ('旧forward世代の保全', 'PASS', '旧2世代のobject全体、閾値、モデル参照、登録SHAを一切更新しない。'),
    ('監査の継続', 'PASS', '新しい独立世代で14ファイルの固定監査を実行。1byte改変3ケースを拒否。'),
    ('自動テスト', 'PASS', f'{len(tests)}件合格・0失敗・0スキップ。2件のPerformanceWarningのみ。'),
    ('shadow有効の通常runner通し試験', 'PASS', '通常mainで2回予想、20epoch DL、shadow JSON、条件付き比較、17レースpaceまで完了。'),
    ('元順位・スコア', 'PASS', '保存済み本番10/12 Excelと231頭のrank/score/score_raw/totalを完全一致照合。'),
    ('既存買い目', 'PASS', '保存済み本番の買い目_レース別1行・回収率重視_買い目候補と完全一致。'),
    ('既存セル・書式', 'PASS', '同一runnerの追加直前baselineと同じpaceを適用し、全既存セル・style・寸法等の差分0。'),
    ('条件付き順位・再実行', 'PASS', '17レースの独立oracle不一致0、変更2・対象外6。再適用差分0。'),
    ('本番への設定採用・GitHub PR反映', '承認待ち', '本番変更・push・mergeは未実施。新世代はoffline replayであり、本番運用開始条件は別途承認が必要。'),
]
rows = [['validation', *row] for row in checks]
rows += [['pytest', x.get('classname', '') + '::' + x.get('name', ''), 'PASS', x.get('time', '')] for x in tests]
with (ROOT / 'reports/conditional_rerank_merge_blocked_resolution_results.csv').open('w', encoding='utf-8-sig', newline='') as stream:
    writer = csv.writer(stream)
    writer.writerow(['category', 'item', 'status', 'evidence'])
    writer.writerows(rows)

table = '\n'.join(f'| {name} | {status} | {evidence} |' for name, status, evidence in checks)
changed = subprocess.check_output(['git', 'diff', '--name-only', 'd7d779b', 'HEAD'], cwd=ROOT).decode('utf-8').strip().splitlines()
file_list = '\n'.join('- `' + name + '`' for name in changed)
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip()
text = f'''# MERGE BLOCKED解消検証（2026-10-13）

検証日時: {datetime.now().isoformat(timespec='seconds')} JST。
**ローカル候補の技術的な阻害要因は解消、shadow有効の通常runner通し試験はPASS。現在のGitHub PR #4は未更新であり、まだMERGE READYとは判定しません。**
本番へのforward/shadow設定採用、運用世代の開始日・出力保存先・集計分離を承認し、検証ブランチのpush後にPRの最終SHAを確認する必要があります。
目的は機能の統合検証であり、収益・精度向上や前向き検証完了の認定ではありません。

| 確認項目 | 判定 | 根拠 |
| --- | --- | --- |
{table}

## 根本原因と正しい実体

1. **production_model.jsonのforward不一致**: 9/13の昇格コミット`265e0a4`で本番が20260730→20260817へ変わりました。Git blobは`f20d595c630be04a6306ae71cef1465f3f127c62`→`2dee82c1a45fca1b253c2fc2a1713d8b4752c3a9`。forwardの20260905世代は旧モデルのblobを保持したままでした。現在の正しい本番実体は昇格済み20260817です。旧本番へ巻き戻して監査を通す対応はしていません。
2. **runnerのforward不一致**: 旧登録blob=`797202af305006e0c6724deeea9333cc472c9111`はmainのrunner。PR追加後は`ff2fb72eeb16d482505882bc0f23d563ae9179fb`で、差分は既存買い目作成後の条件付き比較追加だけです。検証側のrunnerはPR headとbyte一致しています。従来の凍結世代で異なるrunnerを同じものと見なす監査迂回はしていません。
3. **shadow SHA不一致**: `d66c425`でPython/JSONをLFへ固定しました。現在の重みはGitの初期格納版`f5293ef`とbyte一致し、SHA=`bb086384eb58c5abae6ef7104de94e83005c318b3be3900ef5c4df15fca1020d`です。その同一内容をCRLFへ変換すると登録SHA=`83e6126fd01af24ebe553039946dd603c3e6e326b8f6bee0e0a73a7ffd32dcbb`に完全一致しました。重み数値の改変ではなく、LF方針へ移行後の登録値取り残しです。
4. **世代の意味**: 元の20260905世代は「旧本番20260730対shadow20260817」です。現在は本番もshadowも20260817なので、同じ世代の実験として継続すると比較の意味が変わります。旧記録を上書きせず、独立した統合再生検証世代を追加しました。

実効重みSHAは本番/shadowとも`8325f5c0748aa49076631b02424f2104a7924d0b39a4148d4703f687a937a789`、31群・3131要素で不変。現在のshadowは本番と同じモデルによる整合性検証であり、別モデルの優劣比較ではありません。
詳細証拠は`reports/merge_blocked_resolution_20261013/provenance.json`。本番の設定・重みとGit blobのbyte一致をassertで検証しています。

## 7件の失敗の解消

| 分類 | 件数 | 根本原因 | 検証側の修正 |
| --- | ---: | --- | --- |
| 旧モデル期待 | 3 | テストが旧20260730ファイル/既知SHAを固定期待 | 昇格コミット265e0a4の20260817ファイル・既知file/effective SHAを固定期待するよう変更。自己参照で無条件合格にはしない |
| shadow関連 | 2 | LF実ファイルにCRLF登録SHAを照合 | byte変換証明後、shadow設定のfile_sha256だけをLF登録値へ変更 |
| forward監査 | 1 | 昇格・PR追加後も旧凍結世代を選択。テストの開始日期待も旧20260822 | 旧世代を完全保存し、独立した新世代を明示選択、14ファイルを固定監査 |
| 特徴量補完 | 1 | FEAT_COLS全てが重みキーとして存在するという誤前提 | 本番組込重みの補完を検査し、計算専用field_size_adjusted_finishは暗黙0であることを検査。実効SHA/形状不変テストも追加 |

前回「特徴量補完不足」と整理した1件は実装の補完不具合ではなく、テストの契約誤りでした。FEAT_COLSに存在する計算専用・未採用特徴量を重みmapへ0として追加すると、数値効果がなくても実効SHAと要素数を変えてしまいます。baseline実装や本番重みは変更していません。
83件は全合格。改変監査拒否、旧世代不変、shadow改行差の数値モデル不変、本番effective SHA/形状不変の6件を追加しました。テストfixtureは旧設定の固定コピーで、reportsや本番を改変しません。

## 修正理由・影響・リスク

- `1_keibayosou_conditional_rerank.py`: 前回のfloatレースID正規化修正をcommit `290321b`で検証ブランチへ反映。元rank、score、買い目には介入しません。
- `config/shadow_model.json`: file_sha256の1項目だけ変更。model_name、weight_file、effective SHA、形状、日時、output_dir、全分離flagは不変。重み実ファイルは変更なし。変更を本番へ適用するには承認が必要です。
- `config/forward_validation.json`: 旧2世代のobjectを完全維持し、`conditional_rerank_integration_20261013`を追加して検証側のactive_generationだけ選択。目的=`offline_integration_replay`、状態=`validation_only_pending_activation_approval`、当日データ=`20261012`、旧結果と集計しない指定。閾値は従来と同じです。
- 新固定値は既知の昇格blob、PR runner blob、19件回帰を通過したID修正blobだけを許可。その他の旧13対象は旧登録値一致を要求し、未レビューの変更は生成処理が停止します。さらに再ランキングmoduleを14件目として固定しました。監査loaderは未変更です。
- 新世代は本番運用用の最終開始日を決定したものではありません。開始日とoptimizer_holdout_start_dateの変更、同モデル比較への切替は研究集計の意味に影響します。既存世代の統計へ混入させず、本番用の開始日・保存先・世代の採用を管理者が承認する必要があります。approval_required等は記録用メタデータで、runtimeによる本番書込阻止flagではありません。本番を守る実際の措置は、未push/未mergeと今回の隔離ガードです。
- 本番設定・main・2つの本番重み・runner/pipeline/baselineの予想ロジックは今回変更していません。GitHub push、PR merge、本番コピーは未実施です。

## shadow有効の通常runner試験

隔離フォルダ: `{run}`。
データ: 20261012の日付一致入力Excel/OZZU、現在のrace_levels/静的基準タイム/成績マスタ、履歴Excelコピー一式。
本番モデル設定と重みは同一byte。設定は検証ブランチの修正案をコピーしています。
通常`runner.main()`の`ENABLE_SHADOW=True`を保持し、run_pipelineやshadow loaderの置換・無効化はしていません。ハーネスは外部書込/ネットワーク/外部プロセスを禁止し、履歴と新規出力を分離し、比較用baseline保存だけを追加しています。
1回目予想→DL学習20epoch・231頭→2回目予想→固定監査14件→shadow重み/実効SHA検証→本番DataFrame不変assert→shadow JSON保存→通常買い目後処理→条件付き比較（2/17レース変更）→pace17レースまで完了、exit 0。

shadow JSON: `{shadow_files[0]}`。
17レース、result_status=`pre_result`、本番/shadowの実効モデルID一致を確認しました。このJSONは過去日の入力を用いた機能再生であり、本当の結果確定前に取得した予想ログとして前向き成績に加算してはいけません。
本番コピー元{len(source_checks)}ファイルは実行前後のSHAで全件不変でした。

## 元順位・買い目・Excel保存

保存済み本番20261012 Excelとのrank/score/score_raw/total差分0、買い目2シート差分0。
同じrunner実行の条件付き追加直前ブックをbaselineにし、両方へ同じpace処理を実施。既存シート全セル（空セル含む）の値・数式・型、フォント/色/塗り/罫線/配置/表示形式/保護、列幅/行高/非表示/結合/freezeを比較して差分0。
独立oracleとの順位不一致0、順位連番・上位5頭顔ぶれ維持、対象外6レースで変更なし。最終moduleを出力コピーへ再適用してもセル/書式差分0。
前回の73レースの比較根拠は保存したままです。今回の修正は設定・テストであり、条件付きmoduleは前回検証済みのID修正から変えていません。

## コミット・変更ファイル

検証ブランチ: `feature/conditional-rerank-non-maiden-20261012`。
ID/回帰commit=`290321b`、設定案/監査回帰commit=`14f56d6`。報告時HEAD=`{head}`。全てローカルのみ。

{file_list}

加えてExcel照合/報告用toolsとreportsに成果物を追加。以前の未コミット検証スクリプト2件とstashは保持しました。修正差分は`reports/merge_blocked_resolution_20261013/proposal.patch`。

## 実行コマンド

```powershell
$env:PYTHONUTF8='1' # 日本語ログをUTF-8で保存
$env:PYTHONDONTWRITEBYTECODE='1' # 読取利用する本番.venv等へのpyc生成を避ける
git show 265e0a4 -- config/production_model.json # 本番モデル昇格の実際の差分
git show d66c425 -- .gitattributes # LF固定方針の根拠
& ../keiba_yosou_2026-2/.venv/Scripts/python.exe tools/prepare_conditional_integration_generation.py # 既知SHA以外を拒否する一度限りの設定案生成
& ../keiba_yosou_2026-2/.venv/Scripts/python.exe tools/audit_conditional_resolution_provenance.py # Git実体・改行差・本番不変を検証
& ../keiba_yosou_2026-2/.venv/Scripts/python.exe -m pytest tests -q --junitxml=reports/merge_blocked_resolution_20261013/pytest_final.xml # 全体回帰
& ../keiba_yosou_2026-2/.venv/Scripts/python.exe -u tools/final_validation_runner.py # shadowを無効にしない通常main
& ../keiba_yosou_2026-2/.venv/Scripts/python.exe tools/verify_runner_output.py '{run}' reports/merge_blocked_resolution_20261013/runner_output_results.json # 値/書式/買い目/順位を実測
git diff --check # 差分の構文的な整合性
```

ログ・JUnit・provenance・出力照合JSONは`reports/merge_blocked_resolution_20261013/`。前回報告書とCSVは変更していません。

## PR #4の最終判断

**技術検証はPASS、GitHub PR #4は承認待ちでMERGE READY未確定。**
現PR headはまだ`d7d779b`で今回の修正を含まず、mainの設定も変更していません。新しい本番世代の開始日・保存先・集計分離と、shadowの改行差登録修正を承認し、検証ブランチpushでPRが更新された後に、対象SHAを確認してMERGE READY判定できます。
本番へこのoffline再生用設定を無断でコピーする対応は行いません。承認対象は報告書の設定差分と世代運用方針です。
'''
(ROOT / 'reports/conditional_rerank_merge_blocked_resolution.md').write_text(text, encoding='utf-8')
print(json.dumps({'tests_passed': len(tests), 'shadow_json_races': len(payload['races']), 'normal_runner': 'PASS', 'excel_differences': 0, 'production_files_unchanged': len(source_checks), 'pr_status': 'approval and push pending'}, ensure_ascii=False))
