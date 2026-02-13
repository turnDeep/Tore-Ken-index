# Tore-ken (トレけん) Market Dashboard - Configurable Edition

## 1. 概要 (Overview)

**Tore-ken (トレけん)** は、指定した銘柄の市場トレンド（短期・長期）を効率的にモニタリングするためのダッシュボードです。
CSVファイルによる設定駆動型アーキテクチャを採用しており、分析対象の銘柄を柔軟に追加・変更することが可能です。

## 2. 主要機能

### 2.1 Market Analysis (短期トレンド分析)
`short_term_ticker.csv` に設定された銘柄（デフォルト: SPY, QQQ, SOXX, GLD）の日足チャートを表示します。
*   **更新頻度:** 毎日 (火〜土 06:15 JST)
*   **チャート機能:**
    *   ローソク足チャート (6ヶ月分)
    *   TSV (Time Segmented Volume) 近似値
    *   StochRSI & 1OPサイクル判定
    *   Market Bloodbath (暴落シグナル)
    *   トレンド背景色 (緑=上昇, 赤=下落)

### 2.2 Strong Stocks Analysis (長期トレンド分析)
`long_term_ticker.csv` に設定された銘柄（デフォルト: QQQ, SOXX, GLD）の週足チャートを表示します。
*   **更新頻度:** 週1回 (土 06:15 JST)
*   **チャート機能 (Light Mode):**
    *   ローソク足チャート (週足) + ATR Trailing Stop
    *   Zone RS (RRGロジックに基づく相対強度)
    *   RS Percentile (過去のパフォーマンスに対する相対順位)
    *   ※ データ取得期間は過去5年分を基準としています。

## 3. 設定 (Configuration)

以下のCSVファイルを編集することで、表示する銘柄を変更できます。

*   **`short_term_ticker.csv`**: 短期チャート用銘柄リスト
    ```csv
    Ticker
    SPY
    QQQ
    SOXX
    GLD
    ```

*   **`long_term_ticker.csv`**: 長期チャート用銘柄リスト
    ```csv
    Ticker
    QQQ
    SOXX
    GLD
    ```

## 4. 自動実行スケジュール

バックエンドのスケジューラー (`backend/ws_manager.py`) により、米国市場終了後のデータ確定タイミングに合わせて自動更新されます。

*   **実行時間:**
    *   **冬時間:** 日本時間 06:15
    *   **夏時間:** 日本時間 05:15
*   **実行内容:**
    *   **火〜金曜日:** 短期チャートの更新のみ
    *   **土曜日 (金曜引け後):** 短期チャートと長期チャートの両方を更新

## 5. 技術スタック

- **Backend**: Python 3.12, FastAPI
- **Frontend**: HTML5, CSS3, Vanilla JavaScript (PWA対応)
- **Data Processing**: pandas, numpy, yfinance, mplfinance
- **Architecture**:
    - `short_term_process.py`: 短期分析ロジック
    - `long_term_process.py`: 長期分析ロジック
    - `data_fetcher.py`: 処理のオーケストレーション

## 6. ディレクトリ構造

```
.
├── backend/
│   ├── main.py                     # FastAPIアプリケーションサーバー
│   ├── data_fetcher.py             # 統合実行スクリプト
│   ├── short_term_process.py       # 短期チャート生成プロセス
│   ├── long_term_process.py        # 長期チャート生成プロセス
│   ├── rdt_data_fetcher.py         # 長期データ取得ロジック
│   ├── chart_generator_mx.py       # チャート描画エンジン
│   ├── ws_manager.py               # スケジューラー管理
│   ├── stock.csv                   # (内部利用)
│   └── ...
├── frontend/
│   ├── index.html                  # ダッシュボードUI
│   ├── app.js                      # フロントエンドロジック
│   └── style.css                   # スタイルシート
├── short_term_ticker.csv           # 短期チャート設定
├── long_term_ticker.csv            # 長期チャート設定
├── data/                           # 生成データ (Git対象外)
└── README.md
```

## 7. セットアップ手順

### 7.1 インストール

```bash
# 1. リポジトリをクローン
git clone <repository_url>
cd tore-ken

# 2. 依存関係のインストール
pip install -r backend/requirements.txt
```

### 7.2 実行

```bash
# サーバーの起動 (スケジューラーも同時に起動します)
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

ブラウザで `http://localhost:8000` にアクセスしてください。

## 8. ライセンス

本ソフトウェアは個人利用を目的としています。
