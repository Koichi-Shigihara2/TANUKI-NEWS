# MACRO PULSE v6.0

**リポジトリ:** Koichi-Shigihara2/TANUKI-NEWS  
**公開URL:** https://koichi-shigihara2.github.io/TANUKI-NEWS/  
**目的:** 米国株式長期投資における景気フェーズの継続確認

---

## 概要

景気が今どの局面にあるか（拡張・踊り場・後退入口・後退）を、8つの経済指標から機械的に判定するダッシュボードです。自分の感覚と機械的な評価を比較・確認するために使用します。

---

## 画面構成

| セクション | 内容 |
|---|---|
| ① 景気フェーズ判定 | 総合スコアとフェーズ名。8指標の個別シグナル |
| ② 各指標の現在地 | 8指標のバーで現在値と閾値の位置を可視化 |
| ③ 過去後退局面との類似度 | 2019年末・2001年との類似度をレーダーチャートで比較 |
| ④ 直近の動き | 過去2週間の発表実績。サプライズ大きい順 |
| ⑤ 今後2週間のスケジュール | 次に確認すべき発表日一覧 |

---

## データ取得方法

| 指標 | 取得方法 | 頻度 |
|---|---|---|
| Yield Curve 10Y-2Y | FRED自動（GitHub Actions） | 毎日 |
| HY Spread | FRED自動 | 毎日 |
| VIX | FRED自動 | 毎日 |
| Michigan Consumer Sentiment | FRED自動 | 月次（毎朝チェック） |
| Conference Board LEI（OECD CLI） | FRED自動 | 月次（毎朝チェック） |
| ISM Manufacturing PMI | **手動入力**（investing.com） | 月次 |
| ISM Non-Manufacturing PMI | **手動入力**（investing.com） | 月次 |
| Initial Claims 4W MA | FRED自動 | 週次 |

---

## 手動入力データのメンテ方法

### 対象指標

- **ISM Manufacturing PMI**（毎月第1営業日）
- **ISM Non-Manufacturing PMI**（毎月第1週金曜）

### 手順（ISM Manufacturing PMIの例）

#### 1. データ取得

investing.comのISM PMIページ（ https://jp.investing.com/economic-calendar/ism-manufacturing-pmi-173 ）で最新値を確認。

#### 2. CSVに追記

`ism_mfg_import.csv` を開き、最新行を追加：

```csv
release_date,indicator,actual,consensus
2026-04-01,ISM Manufacturing PMI,49.5,50.0
```

- `release_date`：発表日（YYYY-MM-DD形式）
- `actual`：発表された実績値
- `consensus`：事前コンセンサス予想値（不明な場合は空欄）

#### 3. マージスクリプト実行

```powershell
# PowerShellで実行
python merge_ism_to_events.py
```

ISM Non-Manufacturing PMIの場合：

```powershell
python merge_ism_nonmfg_to_events.py
```

#### 4. GitHubへ反映

```powershell
git add data/05_events.csv
git commit -m "add: ISM Mfg PMI 2026-04 手動入力"
git push
```

### 参照URL

| 指標 | URL |
|---|---|
| ISM Manufacturing PMI | https://jp.investing.com/economic-calendar/ism-manufacturing-pmi-173 |
| ISM Non-Manufacturing PMI | https://jp.investing.com/economic-calendar/ism-non-manufacturing-pmi-176 |

---

## ファイル構成

```
TANUKI-NEWS/
├── index.html                        # ダッシュボード本体
├── 05_main.py                        # データ収集メインスクリプト
├── 05_requirements.txt               # Python依存ライブラリ
├── import_michigan_sentiment.py      # Michigan Sentiment過去データ投入
├── import_oecd_cli.py                # OECD CLI過去データ投入
├── merge_ism_to_events.py            # ISM Mfg PMIマージ
├── merge_ism_nonmfg_to_events.py     # ISM Non-Mfg PMIマージ
├── ism_mfg_import.csv                # ISM Mfg 手動入力データ（保管用）
├── .github/workflows/
│   └── 05_update.yml                 # GitHub Actions（Node.js 24対応済み）
└── data/
    ├── 05_events.csv                 # 経済指標イベント蓄積（メインDB）
    ├── 05_economic_history.csv       # 経済履歴
    ├── 05_indicator_schedule.csv     # 発表スケジュール
    └── 05_fed_context.csv            # FED文脈データ
```

---

## GitHub Actions 自動実行スケジュール

| 時刻（JST） | 内容 |
|---|---|
| 毎朝 07:00 | FRED自動取得指標の更新 |
| 毎夜 22:00 | S&P500リターン補完（ret_t1〜ret_t20） |
| 毎週日曜 07:00 | スケジュール更新・FED文脈更新 |

---

## よく使うコマンド（PowerShell）

```powershell
# ISM Mfg PMI マージ
python merge_ism_to_events.py

# ISM Non-Mfg PMI マージ
python merge_ism_nonmfg_to_events.py

# Michigan Sentiment 過去データ投入（FRED APIキー必要）
$env:FRED_API_KEY="your_key"
python import_michigan_sentiment.py

# S&P500リターン手動補完
python 05_main.py --fill-returns

# スケジュール更新
$env:FRED_API_KEY="your_key"
python 05_main.py --update-schedule

# GitHubへ反映（汎用）
git add data/05_events.csv
git commit -m "add: ○○データ投入"
git push

# git競合が起きたとき
git stash
git pull
git stash pop
git push
```

---

## トラブルシューティング

| 症状 | 原因 | 対処 |
|---|---|---|
| `git push` が rejected | GitHub Actionsが先にpushした | `git stash → pull → stash pop → push` |
| `merge conflict in 05_events.csv` | pullで競合発生 | `git checkout --theirs data/05_events.csv → add → commit → push` |
| OECD CLIが古い値のまま | FREDの更新遅延 | 毎朝のActionsが自動補完するため待つ |
| Michigan Sentimentが更新されない | FREDの月次更新遅延 | 毎朝のActionsが自動補完するため待つ |
| ダッシュボードが読み込まれない | CSVパス問題 | ブラウザの開発者ツールでエラー確認 |

---

## スコアリング設計

### フェーズ判定閾値

| スコア範囲 | フェーズ |
|---|---|
| 0〜29 | 拡張 |
| 30〜51 | 踊り場 |
| 52〜69 | 後退入口 |
| 70〜100 | 後退 |

### 各指標のウェイト

| 指標 | ウェイト | 先行期間 |
|---|---|---|
| Yield Curve 10Y-2Y | 20% | 12ヶ月 |
| ISM Manufacturing PMI | 18% | 3ヶ月 |
| HY Spread | 15% | 2ヶ月 |
| ISM Non-Mfg PMI | 12% | 1ヶ月 |
| VIX | 10% | 即時 |
| Initial Claims 4W MA | 10% | 1ヶ月 |
| Michigan Consumer Sentiment | 8% | 2ヶ月 |
| Conference Board LEI（OECD CLI） | 7% | 6ヶ月 |

---

## 残課題

| 優先度 | 内容 |
|---|---|
| 🟢低 | release_date補正確認（他の月次指標） |
| 🟢低 | CB Consumer Confidence旧データ残存（無害だが気になる場合は整理可） |
