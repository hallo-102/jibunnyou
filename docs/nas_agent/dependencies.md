# UGOS本体を変更しない依存の準備方針

2026-11-25の実機読み取り確認。ここにある準備コマンドは**未実行、別承認後のみ**。NASでapt update/install、dpkg -i、global pip、Docker pull/build、既存imageの変更は行わない。

## 初回healthcheckと予想受入を分ける

| 用途 | 採用方針 | 現状/制約 |
|---|---|---|
| agent/healthcheck | NAS既存 `/usr/bin/python3.11` の標準ライブラリ | 3.11.2。pip、ensurepip、予想ライブラリ、venvは不要 |
| agentの版/clean検査・固定SHA取得 | 承認したDebian 12 arm64 GitパッケージをBASE/tools/gitへ展開し、tools/bin/git経由で使用 | NASにGitなし。APTのGit候補もなし。パッケージ版・SHA・依存適合は今後レビューし、未確認なら導入停止 |
| PR17 host helper | 新規BASE/validation-venvだけに依存を配置 | healthcheckの後。ensurepipのないホストでも `venv --without-pip` を使う案。NASでは未作成 |
| 候補予想container | 固定PR17の既存Dockerfile/compose | 現在のagentは受入でbuildを必須とする。既存本番imageへの置換やbuild承認の迂回はしない |

private Gitは[dpkg-debのextract](https://manpages.debian.org/bookworm/dpkg/dpkg-deb.1.en.html)だけを使い、UGOSのdpkgデータベースやmaintainer scriptを操作しない。Debianの署名付きRelease/Packagesから版・Architecture・SHA256を確認したdebを、別の信頼できる環境で準備する。NASのAPT設定/キャッシュは変更しない。

NASにはlibc6、libcurl3-gnutls、libexpat1、libpcre2-8-0、zlib1g、CA certificatesがある。liberror-perlは未導入。clone/fetch/checkout/statusに必要な実依存は、選定Gitのbinaryとgit-remote-httpsに対するlddと公開repoの固定SHA取得で確認する。Perl系追加サブコマンドを要件にしない。不足lib/ABI不一致があれば自動で補わず停止する。

deploy/gitは新規固定prefixのGIT_EXEC_PATH/GIT_TEMPLATE_DIRを設定し、専用binaryだけを起動する。[Gitの公式環境変数仕様](https://git-scm.com/docs/git#_environment_variables)に従う。両systemdテンプレートと手動CLIのPATHをtools/bin優先にそろえる。pollerへDocker権限を渡すGit wrapperは採用しない。

## pip/ensurepip/venvを後回しにする方法

Pythonの[venv --without-pip](https://docs.python.org/3.11/library/venv.html)はensurepipを呼ばず専用環境を作る。host Pythonのsite-packagesを使う `--system-site-packages` は指定しない。既存venvをコピー/移動して再利用しない。

受入用の依存を準備する段階で、承認したpip wheelとLinux ARM64/CPython 3.11用wheelhouse、PR17のnas/requirements.txtに対応した推移依存を含むhash固定requirementsを別環境で用意する。pip wheelのzip importで最初のpipを新規venvへ導入し、その後も新規venv内だけへoffline installする案を採用する。NAS上の互換性は未検証なので実際のpackage名/版/hashを決めてから承認する。

```sh
# 将来の受入準備だけ。healthcheck前には不要。artifactのSHAを先に検証する。
BASE=/volume1/keiba_nas_agent
test ! -e "$BASE/validation-venv"  # 新規環境だけを対象にする。
python3.11 -m venv --without-pip "$BASE/validation-venv"  # UGOSのPythonへpipを入れない。
PIP_WHEEL='<承認・SHA検証済みpip wheelの絶対パス>'
WHEELHOUSE='<承認・SHA検証済みARM64 wheelhouseの絶対パス>'
PYTHONPATH="$PIP_WHEEL" "$BASE/validation-venv/bin/python" -m pip install --no-index --find-links "$WHEELHOUSE" "$PIP_WHEEL"  # private venvへbootstrap。
"$BASE/validation-venv/bin/python" -m pip install --no-index --require-hashes --find-links "$WHEELHOUSE" -r '<全推移依存をhash固定した承認requirements>'
"$BASE/validation-venv/bin/python" -m pip check  # 依存不整合なら受入へ進まない。
```

この方式ならpython3.11-venv/ensurepip/pipをUGOSへpackage導入する必要がない。private Git/venvとも新規領域に限定できない場合は保留し、NAS本体へのapt installを黙って代替実行しない。

## 既存imageの再利用調査

読み取りで確認した本番 `keiba-yosou-2026:arm64` はimage ID `sha256:4172a47d4cfcdaafeafcfc9ad7ee6328927cb644baae1a9c460912057952a269`、linux/arm64、Debian 12、Python 3.11.13。pip/numpy/pandas/openpyxlは存在し、Gitはない。image inspectと既存container内の `python -B` による存在確認のみで、pip処理や予想コードは実行していない。

他にlinux/arm64のPython 3.11系imageはあるが、PR17 Dockerfileの `python:3.11-slim-bookworm` と同じ名前のローカルtagは検出されなかった。浮動tagの取得先digest・依存layer・候補コードSHAの同一性は未証明。既存base/layerは一致確認後にbuild cacheとして再利用できる可能性があるだけで、現時点でoffline build可能とは認定しない。

本番imageにはPR17の候補コード同一性が確認できず、専用host runtimeもまだない。そのため既存imageを候補へそのまま転用しない。稼働中本番containerをpip実行環境にしたり、既存imageをcommit/retagしたり、containerのPython/site-packagesをhostへコピーしたりしない。将来専用helper imageを採る場合も別レビューであり、今回のコードには新しいcontainer経路を追加しない。
