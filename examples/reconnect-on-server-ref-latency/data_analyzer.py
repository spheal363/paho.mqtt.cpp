import pandas as pd

# =========================
# 設定
# =========================
CSV_PATH = "fullcol_2year.csv"   # 入力CSV
TIME_COL = "DATA_AS_OF" # 時刻列
ID_COL   = "ID"         # パブリッシャID

# 急増判定の閾値
DIFF_THRESHOLD  = 10    # 1分で +10 件以上
RATIO_THRESHOLD = 3     # 前分の3倍以上

# =========================
# 読み込み & 前処理
# =========================
df = pd.read_csv(CSV_PATH)

# フォーマットを明示（警告回避）
df[TIME_COL] = pd.to_datetime(
    df[TIME_COL],
    format="%m/%d/%Y %I:%M:%S %p"
)

# =========================
# 1分単位 集計（推奨形）
# =========================
counts_1min = (
    df
    .groupby(
        [
            ID_COL,
            pd.Grouper(key=TIME_COL, freq="1min")
        ]
    )
    .size()
    .rename("count")
    .reset_index()
)

# =========================
# 急増指標
# =========================
# 差分
counts_1min["diff"] = (
    counts_1min
    .groupby(ID_COL)["count"]
    .diff()
)

# 倍率（transform を使う）
counts_1min["ratio"] = (
    counts_1min
    .groupby(ID_COL)["count"]
    .transform(lambda x: x / x.shift(1))
)

# =========================
# 急増箇所抽出
# =========================
spikes = counts_1min[
    (counts_1min["diff"] >= DIFF_THRESHOLD) |
    (counts_1min["ratio"] >= RATIO_THRESHOLD)
]

# =========================
# 出力
# =========================
if spikes.empty:
    print("急激なレコード増加は検出されませんでした。")
else:
    print("急増が検出されたレコード:")
    print(spikes.sort_values([ID_COL, TIME_COL]))

    print("\n急増が観測されたID:")
    print(spikes[ID_COL].unique())