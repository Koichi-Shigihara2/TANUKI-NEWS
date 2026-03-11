# 📊 MACRO PULSE — Economic Indicators Dashboard  v4.0

主要先行経済指標の実際値・前回比トレンド・市場相関を日次自動収集し、GitHub Pages ダッシュボードで可視化する個人用ツール。

---

## 🗂️ ファイル構成

```
.
├── 05_main.py                         # データ収集メインスクリプト (v4)
├── 05_requirements.txt                # Python 依存ライブラリ
├── .github/workflows/05_update.yml   # GitHub Actions (毎朝 07:00 JST)
├── index.html                         # ダッシュボード (GitHub Pages)
├── data/
│   ├── 05_economic_history.csv        # 実績蓄積 CSV（自動生成）
│   └── 05_indicator_schedule.csv         # 発表スケジュール（手動管理）
└── 05_README.md
```

---

## 🚀 セットアップ

### 1. GitHub Secrets の設定

**Settings → Secrets → Actions → New repository secret**

| Secret 名 | 内容 | 優先度 |
|---|---|---|
| `FRED_API_KEY` | [FRED](https://fred.stlouisfed.org/docs/api/api_key.html) 無料発行 | ★★★ 必須 |

> FMP_API_KEY は v4 では使用しません。

### 2. Workflow 書き込み権限

**Settings → Actions → General → Workflow permissions**
→「**Read and write permissions**」を選択して Save

### 3. GitHub Pages の有効化

**Settings → Pages → Source: Deploy from a branch → main / root**

### 4. 動作確認（手動実行）

**Actions タブ → MACRO PULSE — Daily Update → Run workflow**

---

## 📅 週次メンテナンス（5分）

毎週末に `data/05_indicator_schedule.csv` を編集し、翌週の発表予定日を追記します。

```csv
指標名,発表予定日,fred_id,...
Initial Jobless Claims,2026-03-27,ICSA,...
Michigan Consumer Sentiment,2026-04-11,UMCSENT,...
```

**ISM Manufacturing PMI**（FRED 未収録）の場合：
発表後に `actual` 列に実績値を手入力 → GitHub Actions で `--recalc` を実行、またはダッシュボードの「↺ 再計算実行」ボタンを押す。

---

## 💻 ローカル実行

```bash
pip install -r 05_requirements.txt

export FRED_API_KEY="your_key"

python 05_main.py                           # 昨日のデータ取得
python 05_main.py --date 2026-03-10         # 特定日を指定
python 05_main.py --recalc                  # Surprise を全件再計算
python 05_main.py --test                    # テストモード
```

---

## 📋 CSV カラム仕様

| カラム名 | 説明 | 例 |
|---|---|---|
| `指標名` | 指標名称 | ISM Manufacturing PMI |
| `リリース日` | 発表日 (YYYY-MM-DD) | 2026-03-01 |
| `実際値` | 発表実際値 | 50.2 |
| `期待値(Consensus)` | 予想値（ユーザー入力 or 自動代替） | 49.5 |
| `前回値` | 前回発表値 | 48.7 |
| `Surprise(実際-期待)` | 実際値 − 期待値 | 0.7 |
| **`forecast_source`** | **予想値の出所（v4 新規列）** | user / actual_as_forecast |
| `YoY変化(%)` | 前年比変化率 | 3.2 |
| `S&P500` | 当日終値 | 6781.48 |
| `Nasdaq` | 当日終値 | 24956.47 |
| `10Y-2Y(YieldCurve)` | 10年-2年スプレッド | 0.58 |
| `付随データ` | 指標別追加値 (JSON) | {"4W Moving Avg": 218500} |
| `市場反応(自動生成)` | 自動生成テキスト | Positive surprise (+0.700) |
| `データソース` | データ取得元 | FRED |
| `更新日時` | レコード更新日時 | 2026-03-01 07:05:12 |

### forecast_source の値

| 値 | 意味 | Surprise 表示 |
|---|---|---|
| `user` | ユーザーが発表前に入力 | カラー表示（+/-） |
| `user_retroactive` | ユーザーが発表後に事後入力 | カラー表示（緑バッジ） |
| `actual_as_forecast` | 予想なし → 実績=予想で自動代替 | グレーで ±0.00 |
| `stored` | CSV 既存値 | 通常表示 |
| `FRED` | FRED から直接取得 | 通常表示 |
| `none` | No Indicators / YieldCurve 行 | — |

---

## 🖥️ ダッシュボード機能

### 予想入力フロー

```
① 発表前夜 → 指標カードの入力欄に予想値を入力 → [保存] → localStorage に保存
② 翌朝 → GitHub Actions が実際値を CSV に追記 → GitHub Pages に自動反映
③ ブラウザで確認 → CSV実際値 × localStorage予想値 を自動突合 → Surprise 表示
④ 週次レビュー → 予想トラッキング表で精度確認
```

### 事後入力 & 再計算

発表後に予想を入力した場合:
1. 「↺ 再計算実行」バナーが表示される
2. ボタンを押すと全件の Surprise が再計算される
3. `forecast_source` が `user_retroactive` に更新される

---

## 📡 監視対象指標

| 指標名 | FRED ID | 閾値（強気 / 弱気） | 発表頻度 |
|---|---|---|---|
| ISM Manufacturing PMI | — (FRED未収録) | 50 / 50 | 月次 |
| New Residential Starts | HOUST | 140万 / 120万件 | 月次 |
| Durable Goods Orders | DGORDER | — | 月次 |
| Initial Jobless Claims | ICSA | 25万 / 30万件 | 週次 |
| Average Hourly Earnings YoY | AHETPI | — | 月次 |
| Michigan Consumer Sentiment | UMCSENT | 80 / 65 | 月次 |
| Yield Curve 10Y-2Y | T10Y2Y | — | 毎日 |

---

## ⚠️ トラブルシューティング

| 症状 | 原因 | 対処 |
|---|---|---|
| 指標が収集されない | schedule.csv に発表予定日が未記入 | 週末に翌週分を追記 |
| ISM PMI が NaN | FRED 未収録 | schedule.csv の actual 列に手入力後 --recalc |
| Surprise が全部 0 / グレー | 予想未入力 → actual_as_forecast | ダッシュボードで予想を入力 |
| FRED fetch failed | APIキー未設定 | FRED_API_KEY を Secrets に設定 |
| Workflow が自動実行されない | 権限不足 | Read and write permissions を設定 |
