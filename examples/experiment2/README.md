# MQTT負荷試験用Publisher

## 概要

このプログラムは、MQTTブローカーに対する負荷試験を行うためのPublisherです。

- 300パブリッシャ（各ブローカー100個）
- QoS 0、retain false、clean start
- 16バイトペイロード（タイムスタンプ除く）
- 1秒間隔でパブリッシュ
- Ramp-Up 10秒（接続のみ）
- パブリッシュ期間 700秒
- Server Reference対応（RTT測定による最適ブローカー選択）

## ブローカーとトピック

- broker1 (10.20.22.172:1883) → `test_data/1`
- broker2 (10.20.22.173:1883) → `test_data/2`
- broker3 (10.20.22.168:1883) → `test_data/3`

## ビルド方法

**重要**: `examples/experiment2/` ディレクトリ内で直接 `make` を実行しないでください。
CMakeのビルドシステムを使用する必要があります。

### 1. CMakeでビルド設定（初回のみ、または設定変更時）

```bash
cd /home/sdoi/paho.mqtt.cpp
mkdir -p build
cd build

# CMake設定（PAHO_BUILD_EXAMPLESを有効にする）
cmake .. -DPAHO_BUILD_EXAMPLES=ON -DPAHO_WITH_MQTT_C=ON
```

### 2. ビルド実行

```bash
# buildディレクトリ内で実行
cd /home/sdoi/paho.mqtt.cpp/build

# 全体をビルドする場合
make

# または、load_test_publisherのみをビルドする場合
make load_test_publisher
```

### 3. ビルド確認

```bash
# 実行可能ファイルが生成されているか確認
ls -lh build/examples/experiment2/load_test_publisher
```

## 実行方法

### 基本的な実行

```bash
# buildディレクトリ内から実行
cd /home/sdoi/paho.mqtt.cpp/build
./examples/experiment2/load_test_publisher

# または、絶対パスで実行
/home/sdoi/paho.mqtt.cpp/build/examples/experiment2/load_test_publisher
```

### 実行時の動作

1. **Ramp-Up期間（10秒）**
   - 300個のクライアントが均等に分散して接続
   - この期間中はパブリッシュを行わない

2. **パブリッシュ期間（700秒）**
   - 各パブリッシャが1秒間隔でメッセージを送信
   - 各ブローカーに応じたトピック名でパブリッシュ
     - broker1 → `test_data/1`
     - broker2 → `test_data/2`
     - broker3 → `test_data/3`

3. **終了**
   - 700秒経過後、全クライアントを正常に切断して終了

### 実行例

```bash
$ ./examples/experiment2/load_test_publisher
[12:34:56.789] START: MQTT Load Test Publisher
[12:34:56.789] Publishers: 300 (100 per broker)
[12:34:56.789] Ramp-Up: 10 seconds
[12:34:56.789] Publish Duration: 700 seconds
[12:34:56.789] Payload Size: 16 bytes (excluding timestamp)
[12:34:56.789] Created 300 clients
[12:34:56.789] Starting Ramp-Up period (10 seconds)...
[12:34:66.789] Ramp-Up completed. Connected: 300/300
[12:34:66.789] Starting publish period (700 seconds)...
...
[12:45:46.789] Publish period completed
[12:45:46.789] Statistics: Published=210000, Connected=300/300
[12:45:46.789] Disconnecting all clients...
[12:45:49.789] All clients disconnected. Done.
```

## トラブルシューティング

### ビルドエラーが発生する場合

```bash
# クリーンビルドを試す
cd /home/sdoi/paho.mqtt.cpp/build
make clean
cmake .. -DPAHO_BUILD_EXAMPLES=ON -DPAHO_WITH_MQTT_C=ON
make load_test_publisher
```

### 実行時に接続エラーが発生する場合

- ブローカーが起動しているか確認
- ネットワーク接続を確認
- ファイアウォール設定を確認

### 実行を中断する場合

`Ctrl+C` (SIGINT) または `kill` コマンドで中断できます。
