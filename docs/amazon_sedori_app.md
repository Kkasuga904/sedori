# Amazonせどり自動化エージェント

本アプリケーションはAmazon SP-APIとKeepa APIを利用して商品価格を監視し、利益計算と仕入れ判定を自動化します。判定結果がポジティブな場合はSlack/LINE通知、Google Sheets保存、Amazon出品ページへの自動入力を行います。

## 主な機能
- **価格監視**: ASINまたはバーコードから競合価格と過去30日間の価格推移を取得。
- **利益計算**: 仕入れコスト、FBA手数料、送料、税金を加味した利益・ROI・利益率を算出。
- **仕入れ判定**: 設定ファイルの閾値（最低利益額・ROI・ランキング）をもとに自動判定。
- **出品自動化**: Google Sheetsへ記録し、Seleniumで出品フォームに自動入力。
- **通知**: Slack/LINEへ仕入れOK通知を送信。

## 事前準備
1. `config/settings.yml` に各種APIキー・認証情報を記入。
2. GoogleサービスアカウントJSONを `config/env/` 配下に保存し、`settings.yml` の `google_sheets.credentials_file` にパスを指定。
3. ChromeDriverをPATHに追加（Selenium利用時）。
4. Python仮想環境を作成し依存ライブラリをインストール。

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 使い方
```powershell
python -m agents.cli --asin B000123456 --purchase-cost 1200 --shipping-fees 400 --taxes 100 --pretty
```

- `--barcode` でJAN/EANから検索可能。
- `--target-price` を指定すると競合価格ではなく任意の販売価格で利益計算。
- `--env staging` のように指定すると `config/env/staging.yml` を読み込みます。
- `--log-level DEBUG` で詳細ログを有効化。

## テスト
```powershell
pytest
```

## トラブルシューティング
- APIリクエスト上限で失敗した場合は設定ファイルの `runtime.max_retries` と `retry_backoff_seconds` を調整し、十分な待機時間を設けてください。
- Google Sheets/API認証エラーが発生した場合、サービスアカウントの権限とシート共有設定を確認してください。
- Seleniumのログイン手順はAmazonセラーセントラルのUI変更で失敗する可能性があります。`src/services/selenium_uploader.py` 内のセレクタを最新に更新してください。
