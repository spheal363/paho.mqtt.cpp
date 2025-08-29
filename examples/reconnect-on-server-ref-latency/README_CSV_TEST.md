# CSVデータMQTTテスト

このディレクトリには、CSVデータを使用してMQTTブローカーにデータを送信するテストプログラムとスクリプトが含まれています。

## 概要

`reconnect_on_server_ref_latency_test.cpp`は、CSVファイルから交通データを読み込み、複数のMQTTクライアントを使用して時系列順にデータを送信するプログラムです。

## 機能

- **複数クライアント管理**: 最大137個のMQTTクライアントを同時に管理（実際のID数に基づく）
- **時系列データ送信**: CSVの`DATA_AS_OF`フィールドに基づいて実際の時間差でデータを送信（データは既に昇順でソート済み）
- **JSON形式メッセージ**: IDを除く全ての列をJSON形式でパブリッシュ
- **QoS 1送信**: 全メッセージをQoS 1で送信
- **交互ブローカー接続**: クライアント立ち上げ時に「10.20.22.172」と「10.20.22.173」を交互に使用
- **自動再接続**: DISCONNECT受信後の再接続ロジックを維持

## ファイル構成

```
reconnect-on-server-ref-latency/
├── reconnect_on_server_ref_latency_test.cpp    # テスト用C++プログラム
├── test_csv_mqtt.sh                           # テスト実行用シェルスクリプト
├── README_CSV_TEST.md                         # このファイル
└── sample.csv                                 # サンプルCSVデータ
```

## ビルド方法

### 1. 依存関係の確認

必要なパッケージがインストールされていることを確認してください：

```bash
# Ubuntu/Debian
sudo apt-get install cmake make g++ pkg-config

# CentOS/RHEL
sudo yum install cmake make gcc-c++ pkgconfig
```

### 2. プログラムのビルド

```bash
# フルビルド（クリーン、ビルド、実行）
./test_csv_mqtt.sh -f

# ビルドのみ
./test_csv_mqtt.sh -b

# クリーンアップのみ
./test_csv_mqtt.sh -c
```

## 使用方法

### 基本的な使用方法

```bash
# サンプルCSVファイルでテスト実行
./test_csv_mqtt.sh

# 指定したCSVファイルでテスト実行
./test_csv_mqtt.sh your_data.csv

# ヘルプ表示
./test_csv_mqtt.sh -h
```

### オプション

- `-h, --help`: ヘルプを表示
- `-c, --clean`: ビルドディレクトリをクリーンアップ
- `-b, --build`: プログラムをビルド
- `-r, --run`: テストを実行
- `-f, --full`: クリーン、ビルド、実行を順番に実行

## CSVデータ形式

CSVファイルは以下の形式である必要があります：

```csv
ID,SPEED,TRAVEL_TIME,STATUS,DATA_AS_OF,LINK_ID,LINK_POINTS,ENCODED_POLY_LINE,ENCODED_POLY_LINE_LVLS,OWNER,TRANSCOM_ID,BOROUGH,LINK_NAME
329,14.29,383,0,02/07/2020 04:08:03 PM,4329508,"40.75766,-73.99687 40.7604,-74.00328",knwwFlosbMcP`g@yHzVcY\\|}@,BBBB,PA -Lincoln Tunnel,4329508,Manhattan,LINCOLN TUNNEL W CENTER TUBE NY - NJ
```

### 必須フィールド

- `ID`: クライアントIDとして使用（最大400個）
- `DATA_AS_OF`: タイムスタンプ（MM/DD/YYYY HH:MM:SS AM/PM形式）
- その他のフィールド: JSONメッセージに含まれる

## MQTT設定

### ブローカー設定

- **プライマリブローカー**: 10.20.22.172:1883
- **セカンダリブローカー**: 10.20.22.173:1883
- **トピック**: `traffic/data`
- **QoS**: 1

### クライアント設定

- **最大クライアント数**: 137（実際のID数に基づく）
- **クライアントID**: `traffic_client_{ID}`（IDはCSVのIDフィールド）
- **接続方式**: TCP
- **MQTTバージョン**: v5

## 動作の流れ

1. **初期化**: CSVデータを読み込み（データは既に昇順でソート済み）
2. **クライアント作成**: データ数分のMQTTクライアントを作成（最大137個）
3. **ブローカー接続**: クライアントを交互に2つのブローカーに接続
4. **データ送信**: 最古のタイムスタンプを基準として、実際の時間差でデータを送信
5. **メッセージ形式**: IDを除く全ての列をJSON形式でパブリッシュ

## ログ出力

プログラムは以下の情報をログ出力します：

- クライアント接続状況
- メッセージ送信状況
- エラー・警告情報
- タイムスタンプ情報

## トラブルシューティング

### よくある問題

1. **ビルドエラー**
   - 依存関係が不足している可能性があります
   - `./test_csv_mqtt.sh -c -b`でクリーンビルドを試してください

2. **接続エラー**
   - ブローカーが起動していることを確認してください
   - ファイアウォール設定を確認してください

3. **CSVファイルエラー**
   - CSVファイルの形式が正しいことを確認してください
   - ファイルの文字エンコーディングを確認してください

### デバッグ

MQTTトレースを有効にするには：

```bash
export MQTT_C_CLIENT_TRACE=ON
export MQTT_C_CLIENT_TRACE_LEVEL=PROTOCOL
./test_csv_mqtt.sh
```

## ライセンス

このプログラムはEclipse Public License v2.0の下で提供されています。

