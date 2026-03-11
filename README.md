# 📊 Economic Indicators Auto-Update System

主要先行経済指標の「期待値 vs 実際値」を自動収集・比較し、S&P500 / Nasdaq との相関を CSV データベースに蓄積する GitHub Actions パイプラインです。

---

## 🗂️ ファイル構成

```
.
├── main.py
├── requirements.txt
├── .github/
│   └── workflows/
│       └── update.yml
├── data/
│   └── economic_history.csv   ← 自動生成
└── README.md
```

---

## 🚀 セットアップ手順

### 1. リポジトリをフォーク

右上「**Fork**」→ 自分のアカウントへ

---

### 2. GitHub Secrets の設定

**Settings → Secrets and variables → Actions → New repository secret**

| Secret 名 | 内容 | 優先度 |
|---|---|---|
| `FRED_API_KEY` | [FRED](https://fred.stlouisfed.org/docs/api/api_key.html) で無料発行 | ★★★ 必須 |
| `FMP_API_KEY` | [Financial Modeling Prep](https://financialmodelingprep.com/) 無料枠 | ★★☆ 推奨 |

> 現在登録済みの `GEMINI_API_KEY` / `XAI_API_KEY` 等は本システムでは使用しません。

---

### 3. Workflow 書き込み権限の設定

**Settings → Actions → General → Workflow permissions**
→「**Read and write permissions**」を選択して Save

---

### 4. 動作確認（手動実行）

**Actions タブ → Daily Economic Indicators Update → Run workflow**
- `target_date`（任意）: `2024-01-15` 形式で特定日を指定可能

---

## 💻 ローカル実行

```bash
pip install -r requirements.txt

export FRED_API_KEY="your_key"
export FMP_API_KEY="your_key"       # 任意

python main.py                       # 昨日のデータ取得
python main.py --test                # テストモード（スクレイピングスキップ）
python main.py --date 2024-03-01     # 特定日を指定
python main.py --test --date 2024-03-01
```

---

## 📋 CSV カラム仕様

| カラム名 | 説明 | 例 |
|---|---|---|
| `指標名` | 指標名称 | ISM Manufacturing PMI |
| `リリース日` | 発表日 (YYYY-MM-DD) | 2024-01-02 |
| `実際値` | 発表実際値 | 47.4 |
| `期待値(Consensus)` | 市場予測コンセンサス | 47.1 |
| `前回値` | 前回発表値 | 46.7 |
| `Surprise(実際-期待)` | 実際値 − 期待値 | 0.3 |
| `YoY変化(%)` | 前年比変化率 | -5.2 |
| `S&P500` | 当日終値 | 4769.83 |
| `Nasdaq` | 当日終値 | 14992.97 |
| `10Y-2Y(YieldCurve)` | 10年-2年スプレッド | -0.35 |
| `付随データ` | 指標別追加値 (JSON) | {"ISM New Orders": 49.1} |
| `市場反応(自動生成)` | 自動生成テキスト | Positive surprise; Stock up 0.82% |
| `更新日時` | レコード更新日時 | 2024-01-03 07:05:12 |

---

## 📡 監視対象指標

| 指標名 | FRED シリーズ | 付随データ |
|---|---|---|
| ISM Manufacturing PMI | NAPM | 新規受注指数 (NAPMNOI) |
| New Residential Starts | HOUST | 30年住宅ローン金利 (MORTGAGE30US) |
| Durable Goods Orders | DGORDER | 除輸送用機器 (ADXTNO) |
| Initial Jobless Claims | ICSA | 4週移動平均 (IC4WSA) |
| Average Hourly Earnings YoY | AHETPI | CPI前月比との差分 |
| Michigan Consumer Sentiment | UMCSENT | 1年・5年期待インフレ率 |
| Yield Curve 10Y-2Y | T10Y2Y | 毎日記録 |

---

## ⚠️ トラブルシューティング

| 症状 | 原因 | 対処 |
|---|---|---|
| `期待値データ取得不可` | FMP/スクレイピング失敗 | `FMP_API_KEY` を設定する |
| `FRED fetch failed` | APIキー未設定 or 無効 | `FRED_API_KEY` を確認 |
| Workflow が自動実行されない | 権限不足 | Step 3 の書き込み権限を確認 |
| スクレイピングがブロックされる | Investing.com の制限 | FMP API を優先使用（自動フォールバック済み） |
| `NaN` が多い | 当日に指標発表なし | 正常動作（`No Indicators` 行として記録） |

---

## 🔑 必要な API キー（後から追加可能）

現時点では **API キーなしでも起動可能**です（株価・イールドカーブのみ記録）。
精度向上のために、以下を順次追加してください：

1. **`FRED_API_KEY`**（最優先）: 実際値・前回値の取得精度が大幅向上
2. **`FMP_API_KEY`**（次点）: 期待値コンセンサスの取得に必要

取得方法の詳細は上記「GitHub Secrets の設定」を参照してください。
