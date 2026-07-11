# ADR-006: Python・AI比較統合と順位変更上限

- 状態: 採用
- 決定日: 2026-07-10

## 背景

独立AI結果を固定した後でPython予想と比較し、統合順位を作る必要がある。比較時に独立結果を後付け修正したり、AIが存在しない反対材料を作ったり、大幅な順位変更を無審査で採用すると監査性と安全性を失う。

## 決定

1. `stage=independent`、`is_locked=true`、`python_result_visible=false`、hash一致の結果だけを比較元にする。
2. 比較・統合は親の独立runを上書きせず、新しい `AiAnalysis` runへ保存する。
3. 比較入力で初めてPython順位、score、推定馬券内率、期待値、risk、根拠を開示する。
4. 順位差は `Python順位 - 独立AI順位` とし、絶対値3以上を重大不一致とする。
5. 重大不一致がなければ「重大な反対材料なし」とし、反対材料を捏造しない。
6. 統合scoreは0〜100、同点なしの降順とし、統合順位は1位から全頭分連続させる。
7. Python順位からの変更は通常2順位以内、例外4順位以内とする。3順位以上または重大不一致時は手動確認を必須とする。
8. 比較と統合を個別の固定output/hashとして保存し、統合失敗時は比較を `degraded` runとして保持する。
9. 統合結果は提案であり、自動投票へ接続しない。

## Schema・prompt版

- 比較入力: `ai_comparison_input_v1`
- 比較出力: `ai_comparison_result_v1`
- 統合入力: `ai_integration_input_v1`
- 統合出力: `ai_integration_result_v1`
- 比較prompt: `ai-comparison-v1.0.0`
- 統合prompt: `ai-integration-v1.1.0`

`integrated_score`追加前の統合出力は履歴表示のため読取可能とするが、新規runではservice検証で必須とする。

## 結果

- Python、独立AI、比較、統合の各時点で、入力、prompt、出力、hashを追跡できる。
- Providerが馬番、馬名、元順位、順位差、反対材料、統合score、順位変更上限を改変した場合は永続化前に拒否する。
- 大幅な判断変更をUIで手動確認対象として明示できる。

## 影響範囲

- `backend/app/schemas/ai_integration.py`
- `backend/app/services/ai_provider.py`
- `backend/app/services/ai_integration.py`
- `backend/app/api/v1/endpoints/ai.py`
- `backend/app/api/v1/endpoints/jobs.py`
- `backend/app/worker.py`
- `frontend/app/page.tsx`

