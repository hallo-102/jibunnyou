# NAS固定ジョブエージェント（Issue #18）

GitHub Issueを受付として、NAS自身がHTTPS outboundで取得・結果報告する。Windows、外部SSH、Tailscale設定変更、self-hosted Actions runner、任意shell APIを必要としない。

- poller: Docker権限なし。Issue番号/依頼hashだけをspoolへ渡し、集計結果をGitHubへ送る。
- worker: 別ユーザーの固定ランチャー。GitHubから再取得し、依頼元ID/権限・ローカル承認・固定SHA・期限を照合して逐次実行する。
- 初期handler: `healthcheck`、`issue16_acceptance`。後者はPR #17のSHA `3ae9ce0dc5eaf41c2aa5d26c6e62fcef8dc26b92` にあるnas.workspace/launch/candidate/compareを直接利用する。PR #17のマージやコード複製はしない。

**開発成果物のみ。NASへの導入・認証・新サービス起動は管理者の別承認が必要。** 設定例は初期状態で個別承認なし、build不可、重いジョブの時間帯未設定であり、そのままでは受入ジョブを実行しない。

[設計・信頼境界](../docs/nas_agent/design.md) / [初回導入](../docs/nas_agent/install.md) / [依頼・承認・復旧](../docs/nas_agent/operations.md) / [検証・未確認事項](../docs/nas_agent/validation.md)
