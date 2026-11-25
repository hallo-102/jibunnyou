# Issue #16 NAS統合候補

GitHubの共通予想コードを直接利用し、20260817候補だけを隔離して検証する。NAS正式20260730、main、既存データ・定期処理・配信は変更しない。

- [3者差分・関連PRの採用判断](differences.md) / [完全なバイト・LF正規化SHA一覧](source_inventory.json)
- [設計・順位/モデル/前向き契約](design.md) / [再利用元SHA](reused_sources.json)
- [再現・実機受入手順](runbook.md)
- [検証結果・未検証事項](validation.md) / [実開催日コピーの比較集計](real_day_summary.json)
- [将来の切替・切戻し・後続案件](transition.md)
- [PR #17のS1/S2/S3/A1/A2への修正内容](pr17_review.md)
- [PR #17修正後の代表日・統制比較の検証集計](review_summary.json)

候補の起点は `nas.workspace` の新規初期化と、ホストマウント先を検査する `nas.launch`。`nas.candidate` / 比較 / 旧NAS再生にもworkspace指定が必須。実データはGit管理外の固定snapshotから読み、専用カテゴリにのみ出力する。旧版の任意output/直接compose起動の手順は使用しない。ARM64実機・稼働中NAS・Linux/Windows一致・新世代前向き運用は未検証のため、本番切替には進まない。
