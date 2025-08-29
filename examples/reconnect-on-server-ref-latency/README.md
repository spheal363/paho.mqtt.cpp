# MQTT Publisher with Server Reference Reconnection

このサンプルアプリケーションは、MQTT v5のServer Reference機能を使用して、ブローカーからの切断時に自動的に最適なブローカーに再接続するPublisherを実装しています。

## 機能

- **MQTT v5 Publisher**: Server Reference情報を処理
- **自動再接続**: ブローカー切断時に代替ブローカーを自動選択
- **レイテンシ測定**: Ping機能を使用してブローカーの応答時間を測定
- **最適ブローカー選択**: 最も短いレイテンシのブローカーに再接続
- **エラーハンドリング**: 再接続試行回数の制限とバックオフ機能

## 動作原理

1. **Server Reference受信**: Brokerから`USE_ANOTHER_SERVER (0x9c)`または`SERVER_MOVED (0x9d)`の理由コードで切断される
2. **Server Reference解析**: 切断プロパティから代替ブローカーのアドレスリストを抽出
3. **レイテンシ測定**: 各ブローカーに対してpingを実行して応答時間を測定
4. **最適ブローカー選択**: 最も短いレイテンシのブローカーを選択
5. **自動再接続**: 選択されたブローカーに新しいクライアントで接続
6. **Publisher再開**: 再接続後にメッセージ送信を再開

## ビルド方法

```bash
# プロジェクトルートディレクトリで
mkdir build && cd build
cmake ..
make reconnect_on_server_ref_latency
```

## 実行方法

```bash
# 基本的な実行（デフォルトブローカー: localhost:1883）
./reconnect_on_server_ref_latency

# カスタムブローカーを指定
./reconnect_on_server_ref_latency mqtt://broker.example.com:1883

# カスタムクライアントIDを指定
./reconnect_on_server_ref_latency mqtt://broker.example.com:1883 my_client_id
```

## 設定可能な定数

```cpp
// 最大再接続試行回数
const int MAX_RECONNECT_ATTEMPTS = 5;

// 再接続試行間のバックオフ時間（ミリ秒）
const int RECONNECT_BACKOFF_MS = 1000;

// Ping測定のタイムアウト時間（ミリ秒）
const int PING_TIMEOUT_MS = 2000;
```

## ログ出力

アプリケーションは詳細なログを出力し、各操作にタイムスタンプを付けて記録します：

```
[14:30:25.123] Initializing for server 'mqtt://localhost:1883'...
[14:30:25.125] Creating initial client...
[14:30:25.127] Connecting...
[14:30:25.130] Connected to new server successfully
[14:30:25.132] Publisher started. Press 'q' to quit.
[14:30:25.135] Publishing message #1 to topic: test/topic
[14:30:25.138] Message #1 published successfully
```

## エラーハンドリング

- **再接続制限**: 最大試行回数に達すると再接続を停止
- **バックオフ**: 連続した再接続試行の間に適切な待機時間を設定
- **例外処理**: 接続エラーやクライアント作成エラーを適切に処理
- **スレッド安全**: クライアントポインタの操作をmutexで保護

## 依存関係

- **Paho MQTT C++**: MQTTクライアントライブラリ
- **C++17**: モダンC++機能を使用
- **POSIX**: pingコマンドを使用したレイテンシ測定

## テスト方法

1. **複数ブローカー環境**: 複数のMQTTブローカーを起動
2. **Server Reference設定**: ブローカーにServer Referenceプロパティを設定
3. **切断テスト**: ブローカーを停止して自動再接続をテスト
4. **レイテンシ測定**: 異なるネットワーク条件での動作確認

## 注意事項

- **pingコマンド**: システムにpingコマンドが利用可能である必要があります
- **権限**: ネットワーク測定のため、適切なネットワーク権限が必要です
- **ファイアウォール**: ブローカーへの接続がファイアウォールで許可されている必要があります

## トラブルシューティング

### 再接続が動作しない場合
1. Server Referenceプロパティが正しく設定されているか確認
2. 代替ブローカーが到達可能か確認
3. pingコマンドが利用可能か確認

### レイテンシ測定が失敗する場合
1. ネットワーク権限を確認
2. ファイアウォール設定を確認
3. ブローカーのホスト名が解決できるか確認

## ライセンス

このサンプルはEclipse Public License v2.0とEclipse Distribution License v1.0の下で提供されています。

