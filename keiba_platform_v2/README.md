# Keiba Platform V2

既存システムとは独立した、競馬予想・オッズ分析・検証プラットフォーム。

## 方針

- 既存 `jibunnyou` 配下コードを変更しない。
- この `keiba_platform_v2/` 内だけで完結する。
- まず SHADOW 運用で検証し、本番投票は明示的なゲートを通す。
- データ品質、モデル追跡、再現性を予想精度と同じくらい重視する。

## 構成

```text
keiba_platform_v2/
├─ config/
│  └─ settings.yaml
├─ src/keiba_v2/
│  ├─ cli.py
│  ├─ config.py
│  ├─ contracts.py
│  ├─ validation.py
│  ├─ prediction.py
│  ├─ odds.py
│  ├─ shadow.py
│  ├─ tracking.py
│  └─ orchestrator.py
├─ tests/
│  └─ test_core.py
├─ data/
│  ├─ input/.gitkeep
│  ├─ output/.gitkeep
│  └─ runtime/.gitkeep
├─ pyproject.toml
└─ README.md
```

## MVPでできること

1. CSV/Excelのレース入力を読み込む。
2. 必須列・race_id・馬番・オッズ・重複を検査する。
3. LightGBMが利用可能なら学習済みモデルを読み、なければ再現可能なルールベースでスコアを作る。
4. 単勝オッズ分布から集中率・断層・期待値候補を算出する。
5. SHADOW買い目を生成し、JSONLで監査ログを残す。
6. MLflowが利用可能なら実験・指標を記録する。未導入時も処理は継続する。
7. 一連の処理をCLIから1コマンドで実行する。

## セットアップ

```powershell
cd keiba_platform_v2
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .[ml,dev]
```

## 実行

```powershell
keiba-v2 validate --input data/input/races.xlsx
keiba-v2 run --input data/input/races.xlsx --date 20260919
```

## 入力の最低限必要な列

- `race_id`
- `horse_no`
- `horse_name`
- `win_odds`

任意で `feature_*` 列を追加すると予想スコアに利用できる。

## 本番化ロードマップ

- Phase 1: 入力検査・予想・オッズ分析・SHADOW
- Phase 2: 結果取込・ROI/WALK-FORWARD評価・MLflow比較
- Phase 3: Prefectによる収集→予想→結果→検証のオーケストレーション
- Phase 4: JRA/netkeiba取得アダプタ追加
- Phase 5: 既存システムとは独立したLIVEゲートを実装し、十分なSHADOW実績後のみ有効化
