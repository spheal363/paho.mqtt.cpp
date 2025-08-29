// reconnect_on_server_ref_latency_test.cpp
//
// This is a Paho MQTT C++ client, sample application for testing with CSV data.
//
// This application demonstrates:
//  - CSV data streaming with 100,000x time acceleration
//  - QoS0 for all messages (high speed)
//  - Automatic reconnection to best broker based on ping latency
//  - Server Reference handling for MQTT v5
//

#include <iostream>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <memory>
#include <functional>
#include <iomanip>
#include <sstream>
#include <vector>
#include <fstream>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <csignal>
#include <regex>
#include <unordered_map>
#include <climits>
#include <unordered_set>
#include <algorithm>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include <mqtt/async_client.h>
#include <mqtt/connect_options.h>
#include <mqtt/properties.h>
#include <mqtt/reason_code.h>

using namespace std;

// Constants
const string TOPIC = "traffic/data";
const int SPEED_MULTIPLIER = 40000; // 40,000倍のスピード

// 許可されたID（138個のみ処理対象）
static const std::unordered_set<int> kAllowedIds = {
    1,106,110,119,124,126,129,137,140,141,142,145,148,149,150,
    153,154,155,157,159,160,164,165,167,168,169,170,171,172,
    177,178,184,185,186,190,191,195,199,2,202,204,205,206,207,
    208,211,212,213,215,217,221,222,223,257,258,259,261,262,
    263,264,265,295,298,3,311,315,318,319,324,325,329,330,331,
    332,338,339,344,345,347,349,350,351,354,364,365,369,375,376,
    377,378,379,380,381,382,383,384,385,387,388,390,394,395,
    398,399,4,402,405,406,410,411,412,413,416,417,418,419,422,
    423,424,425,426,427,428,430,431,433,434,435,436,437,439,
    440,441,445,448,450,451,453
};

// 簡易セマフォ（inflight制御用）
class CountingSemaphore {
    std::mutex m; 
    std::condition_variable cv; 
    int cnt;
public:
    explicit CountingSemaphore(int n): cnt(n) {}
    void acquire() { 
        std::unique_lock<std::mutex> lk(m); 
        cv.wait(lk,[&]{return cnt>0;}); 
        --cnt; 
    }
    void release() { 
        std::lock_guard<std::mutex> lk(m); 
        ++cnt; 
        cv.notify_one(); 
    }
};

// CSVデータ構造（必要最小限）
struct TrafficData {
    int id;                    // ID（数値）
    string raw_csv_line;       // 生CSV行（JSON構築を避ける）
    chrono::system_clock::time_point timestamp;  // タイムスタンプ
    
    TrafficData() : id(0), timestamp(chrono::system_clock::now()) {}
};

// 文字列エスケープ関数（現在は使用していないが、将来の拡張用に保持）
/*
string escape_json_string(const string& input) {
    string result;
    result.reserve(input.length() * 2);
    
    for (char c : input) {
        switch (c) {
            case '"':  result += "\\\""; break;
            case '\\': result += "\\\\"; break;
            case '\b': result += "\\b";  break;
            case '\f': result += "\\f";  break;
            case '\n': result += "\\n";  break;
            case '\r': result += "\\r";  break;
            case '\t': result += "\\t";  break;
            default:
                if (c >= 0x20 && c <= 0x7E) {
                    result += c;
                } else {
                    char hex[8];
                    snprintf(hex, sizeof(hex), "\\u%04x", (unsigned char)c);
                    result += hex;
                }
                break;
        }
    }
    return result;
}
*/

// タイムスタンプ取得関数
string get_timestamp() {
    auto now = chrono::system_clock::now();
    auto time_t = chrono::system_clock::to_time_t(now);
    auto ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;
    
    stringstream ss;
    ss << put_time(localtime(&time_t), "%H:%M:%S");
    ss << "." << setfill('0') << setw(3) << ms.count();
    return ss.str();
}

// 高分解能スリープ（サブミリ秒精度）
inline void hires_sleep_until(std::chrono::steady_clock::time_point tp) {
    using namespace std::chrono;
    // まず "早すぎる"なら余裕を残して少しだけ寝る（カーネル任せ）
    auto now = steady_clock::now();
    if (tp > now) {
        auto early = tp - now;
        if (early > 2ms) {
            // 2ms 手前までだけ寝る（粗いフェーズ）
            auto t_coarse = tp - 2ms;
            timespec ts;
            auto ns = duration_cast<nanoseconds>(t_coarse.time_since_epoch()).count();
            ts.tv_sec  = ns / 1000000000LL;
            ts.tv_nsec = ns % 1000000000LL;
            clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, nullptr);
        }
    }

    // 微小区間は busy-wait（CPUに優しく _mm_pause か yield）
    while (steady_clock::now() < tp) {
        std::this_thread::yield(); // 可能なら _mm_pause() でもOK
    }
}

// URI正規化関数
static std::string normalize_tcp_uri(std::string s) {
    // 前後空白除去
    auto l = s.find_first_not_of(" \t");
    auto r = s.find_last_not_of(" \t");
    s = (l==std::string::npos) ? "" : s.substr(l, r-l+1);

    // スキーム付与
    if (s.rfind("tcp://", 0) != 0) s = "tcp://" + s;

    // ホストとポートの抽出（tcp:// を除去して判定）
    std::string rest = s.substr(6);
    if (rest.find(':') == std::string::npos) {
        s += ":1883";
    }
    return s;
}

// 1回のTCP connect RTT測定（内部関数）
int measure_rtt_once(const string& host, int port, int timeout_ms) {
    // ソケット作成
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        return -1; // 失敗
    }
    
    // 非ブロッキングモードに設定
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags == -1) {
        close(sock);
        return -1;
    }
    if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1) {
        close(sock);
        return -1;
    }
    
    // アドレス構造体を設定
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    // ホスト名をIPアドレスに変換
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        // ホスト名の場合はgetaddrinfoを使用
        struct addrinfo hints, *result;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        
        if (getaddrinfo(host.c_str(), nullptr, &hints, &result) != 0) {
            close(sock);
            return -1;
        }
        
        // 最初の結果を使用
        struct sockaddr_in* addr_in = (struct sockaddr_in*)result->ai_addr;
        addr.sin_addr = addr_in->sin_addr;
        freeaddrinfo(result);
    }
    
    // 接続開始時刻を記録
    auto start_time = std::chrono::steady_clock::now();
    
    // 非ブロッキング接続を試行
    int connect_result = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
    
    if (connect_result == 0) {
        // 即座に接続成功
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        close(sock);
        return static_cast<int>(duration.count());
    }
    
    if (errno != EINPROGRESS) {
        // 接続失敗
        close(sock);
        return -1;
    }
    
    // EINPROGRESSの場合、selectでタイムアウト監視
    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(sock, &write_fds);
    
    struct timeval timeout;
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;
    
    int select_result = select(sock + 1, nullptr, &write_fds, nullptr, &timeout);
    
    if (select_result <= 0) {
        // タイムアウトまたはエラー
        close(sock);
        return -1;
    }
    
    // 接続状態を確認
    int error = 0;
    socklen_t error_len = sizeof(error);
    if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, &error_len) == -1 || error != 0) {
        // 接続失敗
        close(sock);
        return -1;
    }
    
    // 接続成功、経過時間を計算
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    close(sock);
    return static_cast<int>(duration.count());
}

// TCP connect方式のRTT測定関数（3回繰り返し、平均値を返す）
int measure_rtt_ms(const string& host_or_uri, int timeout_ms = 1000) {
    // URIからホストとポートを抽出
    string host = host_or_uri;
    int port = 1883; // デフォルトポート
    
    if (host.substr(0, 6) == "tcp://") {
        host = host.substr(6);
    }
    
    size_t colon_pos = host.find(':');
    if (colon_pos != string::npos) {
        string port_str = host.substr(colon_pos + 1);
        try {
            port = stoi(port_str);
        } catch (...) {
            port = 1883; // パース失敗時はデフォルト
        }
        host = host.substr(0, colon_pos);
    }
    
    // 3回の計測を実行
    vector<int> successful_measurements;
    successful_measurements.reserve(3);
    
    for (int attempt = 0; attempt < 3; ++attempt) {
        int rtt = measure_rtt_once(host, port, timeout_ms);
        if (rtt >= 0) {
            successful_measurements.push_back(rtt);
        }
        
        // 最後の試行でない場合は少し待機（連続接続を避ける）
        if (attempt < 2) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    
    // 成功した計測結果がない場合はタイムアウト値を返す
    if (successful_measurements.empty()) {
        return timeout_ms;
    }
    
    // 成功した計測結果の平均を計算
    int sum = 0;
    for (int rtt : successful_measurements) {
        sum += rtt;
    }
    
    return sum / successful_measurements.size();
}

// ブローカー選択用のRTT計測関数
string select_best_broker(const vector<string>& broker_urls, int timeout_ms = 1000) {
    if (broker_urls.empty()) {
        return "";
    }
    if (broker_urls.size() == 1) {
        return broker_urls[0];
    }
    
    cout << "[" << get_timestamp() << "] Measuring RTT for broker selection..." << endl;
    
    string best_broker = broker_urls[0];
    int best_rtt = INT_MAX;
    
    for (const auto& broker : broker_urls) {
        int rtt = measure_rtt_ms(broker, timeout_ms);
        cout << "[" << get_timestamp() << "] " << broker << " RTT: " << rtt << "ms" << endl;
        
        if (rtt < best_rtt) {
            best_rtt = rtt;
            best_broker = broker;
        }
    }
    
    cout << "[" << get_timestamp() << "] Selected best broker: " << best_broker << " (RTT: " << best_rtt << "ms)" << endl;
    return best_broker;
}

// クライアント管理クラス
class MqttClientManager {
private:
    class ClientCallback;

    string client_id;
    string server_uri;
    mqtt::async_client* client;
    std::thread pub_thread;
    std::shared_ptr<MqttClientManager::ClientCallback> callback_;
    atomic<bool> quit{false};
    atomic<bool> connected{false};
    mutex client_mutex;
    
    // 送信キュー
    queue<TrafficData> message_queue;
    mutex queue_mutex;
    condition_variable queue_cv;
    
    // 再接続用のコールバック
    function<void(const string&)> reconnect_func_;
    
    // 競合防止用フラグ
    std::atomic<bool> reconnecting_{false};
    
    // 再利用可能なメッセージオブジェクト（メモリ確保削減）
    mqtt::message_ptr reusable_message;
    
    // 再接続で削除された件数を記録するカウンタ
    std::atomic<size_t> dropped_count{0};

public:
    MqttClientManager(const string& id, const string& uri, function<void(const string&)> reconnect_cb)
        : client_id(id), server_uri(uri), client(nullptr), reconnect_func_(reconnect_cb) {}
    
    ~MqttClientManager() {
        if (quit.load()) return;
        cleanup();
    }
    
    // 削除された件数を取得するゲッター
    size_t get_dropped_count() const {
        return dropped_count.load();
    }
    
    void cleanup(bool full = true) {
        connected.store(false);        // 明示的に落とす
        quit.store(true);
        queue_cv.notify_all();
        
        if (pub_thread.joinable()) {
            pub_thread.join();
        }
        
        if (client) {
            try {
                if (client->is_connected()) {
                    auto disconnect_token = client->disconnect();
                    if (disconnect_token) {
                        disconnect_token->wait_for(chrono::seconds(3));
                    }
                }
                delete client;
                client = nullptr;
            } catch (...) {
                if (client) {
                    delete client;
                    client = nullptr;
                }
            }
        }
        
        if (!full) {
            // 再接続用のcleanupの場合
            // キューをクリア（再接続中に溜まったデータは破棄）
            size_t cleared = 0;
            {
                lock_guard<mutex> lock(queue_mutex);
                while (!message_queue.empty()) {
                    message_queue.pop();
                    ++cleared;
                }
            }
            
            // 削除件数を累計に加算
            dropped_count += cleared;
            
            // quitフラグをfalseに戻して再利用可能にする
            quit.store(false);
            cout << "[" << get_timestamp() << "] Reset flags for reconnection - queue cleared (" << cleared << " messages), total dropped: " << dropped_count.load() << ", finished=false, quit=false" << endl;
        } else {
            // 完全終了用のcleanupの場合
            {
                lock_guard<mutex> lock(queue_mutex);
                while (!message_queue.empty()) {
                    message_queue.pop();
                }
            }
        }
    }
    
    bool is_connected() const {
        return connected.load();
    }
    
    void add_message(const TrafficData& data) {
        try {
            lock_guard<mutex> lock(queue_mutex);
            if (!quit.load()) {
                message_queue.push(data);
                queue_cv.notify_one();
            }
        } catch (...) {
            // エラーは無視
        }
    }
    
    void start() {
        create_client();
    }
    
    void reconnect(const string& new_uri) {
        if (reconnecting_.exchange(true)) {
            cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Reconnect already in progress. Skip." << endl;
            return;
        }
        
        cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Reconnection started, clearing queue" << endl;
        
        {
            lock_guard<mutex> lock(client_mutex);
            server_uri = new_uri;
        }
        
        // 既存のクライアントとpublisher_threadをクリーンアップ
        cleanup(false);     // キューをクリアし、フラグをリセット
        
        // cleanup(false)内でdisconnect完了を確認済みなので、すぐに新しいクライアントを作成
        create_client();
        
        // 成功・失敗どちらでもフラグをリセット
        reconnecting_.store(false);
        
        cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Reconnected, waiting for new messages" << endl;
    }
    
private:
    void create_client() {
        try {
            client = new mqtt::async_client(server_uri, client_id);
            
            callback_ = std::make_shared<MqttClientManager::ClientCallback>(this);
            client->set_callback(*callback_.get());
            callback_->setup_disconnected_handler(*client);
            
            auto connect_options = mqtt::connect_options_builder()
                .mqtt_version(MQTTVERSION_5)
                .clean_start(true)
                .connect_timeout(chrono::seconds(10))   // 10秒に延長
                .keep_alive_interval(chrono::seconds(15))
                .finalize();
            
            auto connect_token = client->connect(connect_options);
            
            // 無期限待ち（タイムアウトをconnect_token側に任せる）
            connect_token->wait();
            
            if (connect_token->get_return_code() == 0) {
                cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Connected: " << server_uri << endl;
                connected.store(true);
                
                // 再接続完了時：基準時間をリセット（キューは保持）
                // t0_wall と t0_data は削除されたため、ここでは何もしない
                
                // キューは保持（古いデータはpublisher_thread側でSKIPして自然に破棄）
                cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Reconnected: queue empty (cleared)" << endl;
                
                // 再利用可能なメッセージオブジェクトを作成
                reusable_message = mqtt::make_message(TOPIC, "", 0, false);
                
                // 必ず新しいpublisher_threadを起動
                quit.store(false);
                
                // publisher_threadを即座に起動
                pub_thread = std::thread(&MqttClientManager::publisher_thread, this);
                cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Publisher thread started" << endl;
                
            } else {
                cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Connect failed, rc=" << connect_token->get_return_code()
                     << " uri=" << server_uri << endl;
                delete client;
                client = nullptr;
                
                // 接続失敗時は再接続を試行
                if (reconnect_func_) {
                    cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Attempting reconnection due to connect failure..." << endl;
                    reconnect_func_(server_uri);
                }
            }
        } catch (const std::exception& e) {
            cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - EXCEPTION in create_client: " << e.what()
                 << " uri=" << server_uri << endl;
            if (client) { delete client; client = nullptr; }
            
            // 例外発生時も再接続を試行
            if (reconnect_func_) {
                cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Attempting reconnection due to exception..." << endl;
                reconnect_func_(server_uri);
            }
        } catch (...) {
            cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - UNKNOWN EXCEPTION in create_client, uri=" << server_uri << endl;
            if (client) { delete client; client = nullptr; }
            
            // 未知の例外発生時も再接続を試行
            if (reconnect_func_) {
                cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Attempting reconnection due to unknown exception..." << endl;
                reconnect_func_(server_uri);
            }
        }
    }
    
    void publisher_thread() {
        while (!quit.load()) {
            TrafficData data;
            bool has_data = false;
            
            {
                unique_lock<mutex> lock(queue_mutex);
                while (!quit.load() && message_queue.empty()) {
                    queue_cv.wait(lock);
                }
                if (quit.load()) break;
                data = message_queue.front();
                message_queue.pop();
                has_data = true;
            }
            
            if (quit.load() || !has_data) break;
            
            // キューにデータが入ったら即座にpublish（時間制御はmain側で完了済み）
            // クライアントの状態をチェック
            bool is_connected = false;
            mqtt::async_client* current_client = nullptr;
            
            {
                lock_guard<mutex> lock(client_mutex);
                if (client && connected.load()) {
                    is_connected = true;
                    current_client = client;
                }
            }
            
            if (!is_connected || !current_client) {
                // 再接続で削除された場合はカウントを増やす
                if (dropped_count.fetch_add(1) == 0) { // 初めて削除された場合のみログ出力
                    cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Dropped message due to client disconnection." << endl;
                }
                continue;
            }
            
            try {
                const std::string& payload = data.raw_csv_line;
                
                if (payload.empty() || payload.length() > 10000) {
                    continue;
                }
                
                bool publish_success = false;
                {
                    lock_guard<mutex> client_lock(client_mutex);
                    
                    if (client && connected.load()) {
                        try {
                            if (!client->is_connected()) {
                                continue;
                            }
                            
                            // 再利用可能なメッセージのpayload部分だけ書き換え（メモリ確保削減）
                            if (reusable_message) {
                                reusable_message->set_payload(payload);
                                client->publish(reusable_message);
                                publish_success = true;
                            } else {
                                // フォールバック：直接publish
                                client->publish(TOPIC, payload, 0, false);
                                publish_success = true;
                            }
                            
                        } catch (...) {
                            // エラーは無視
                        }
                    }
                }
                
                if (!publish_success) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
                
            } catch (...) {
                // エラーは無視
            }
        }
        
        // publisher_thread終了時（finishedフラグは削除されたため、何もしない）
    }
    
    // 内部コールバッククラス
    class ClientCallback : public mqtt::callback {
    private:
        MqttClientManager* manager;
        
    public:
        ClientCallback(MqttClientManager* mgr) : manager(mgr) {}
        
        void connection_lost(const string& cause) override {
            manager->connected.store(false);
            
            // 別スレッドに移譲してレースを防ぐ
            std::thread([this]() {
                cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Background thread: Connection lost, attempting reconnection..." << endl;
                if (manager->reconnect_func_) {
                    string normalized_uri = normalize_tcp_uri(manager->server_uri);
                    manager->reconnect_func_(normalized_uri);
                }
            }).detach();
        }
        
        void delivery_complete(mqtt::delivery_token_ptr tok) override {
            // QoS0の場合は何もしない
        }
        
        void setup_disconnected_handler(mqtt::async_client& client) {
            client.set_disconnected_handler([this](const mqtt::properties& props, const mqtt::ReasonCode& reason) {
                if (reason == mqtt::ReasonCode::USE_ANOTHER_SERVER || reason == mqtt::ReasonCode::SERVER_MOVED) {
                    cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Server reference received" << endl;
                    
                    vector<string> server_addresses;
                    
                    for (const auto& prop : props) {
                        if (prop.type() == mqtt::property::SERVER_REFERENCE) {
                            string server_ref = get<string>(prop);
                            size_t pos = 0;
                            while (pos < server_ref.length()) {
                                size_t comma_pos = server_ref.find(',', pos);
                                if (comma_pos == string::npos) {
                                    comma_pos = server_ref.length();
                                }
                                
                                string server = server_ref.substr(pos, comma_pos - pos);
                                server = normalize_tcp_uri(server);  // URI正規化
                                if (!server.empty()) {
                                    server_addresses.push_back(server);
                                }
                                pos = comma_pos + 1;
                            }
                            break;
                        }
                    }
                    
                    if (!server_addresses.empty()) {
                        cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Server reference received. Try [";
                        for (size_t i = 0; i < server_addresses.size(); ++i) {
                            if (i > 0) cout << ",";
                            cout << server_addresses[i];
                        }
                        cout << "]" << endl;
                        
                        // 各候補に対してRTTを測定して最適なブローカーを選択
                        string best_server = select_best_broker(server_addresses, 2000); // タイムアウトを2秒に延長
                        
                        if (!best_server.empty()) {
                            cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Selected best server from reference: " << best_server << endl;
                            
                            // 別スレッドに移譲してレースを防ぐ
                            std::thread([this, best_server]() {
                                cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Background thread: Starting reconnection to " << best_server << endl;
                                manager->reconnect(best_server);
                            }).detach();
                        } else {
                            cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Failed to select best server from reference, using first one" << endl;
                            
                            // フォールバック：最初のサーバーを使用
                            string fallback_server = normalize_tcp_uri(server_addresses[0]);
                            std::thread([this, fallback_server]() {
                                cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Background thread: Starting fallback reconnection to " << fallback_server << endl;
                                manager->reconnect(fallback_server);
                            }).detach();
                        }
                    } else {
                        cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - No valid server addresses in reference" << endl;
                    }
                }
            });
        }
    };
};

// CSVプロセッサー（ヘッダー名を使用）
class CsvProcessor {
private:
    ifstream file_;
    vector<string> headers_;
    int idx_id_ = -1, idx_ts_ = -1;
    bool use_ampm_format_ = false; // AM/PM形式かどうか
    
public:
    CsvProcessor(const string& filename) {
        file_.open(filename);
        if (!file_.is_open()) {
            throw runtime_error("Cannot open file: " + filename);
        }
        
        // ヘッダー行を読み取り
        string header_line;
        getline(file_, header_line);
        
        string field;
        stringstream hs(header_line);
        for (int i = 0; getline(hs, field, ','); ++i) {
            headers_.push_back(field);
            if (field == "ID") idx_id_ = i;
            if (field == "DATA_AS_OF") idx_ts_ = i;
        }
        
        // idx_id_ と idx_ts_ が見つからなければエラー扱いでもOK
        if (idx_id_ == -1 || idx_ts_ == -1) {
            cout << "[" << get_timestamp() << "] Warning: ID or DATA_AS_OF column not found" << endl;
        }
        
        // 最初のデータ行を読んでAM/PM形式かどうかを検出
        string first_data_line;
        if (getline(file_, first_data_line)) {
            // 最初の行をバッファに戻す
            file_.clear();
            file_.seekg(-static_cast<long>(first_data_line.length() + 1), ios::cur);
            
            // AM/PM形式かどうかを判定
            use_ampm_format_ = (first_data_line.find("AM") != string::npos || 
                               first_data_line.find("PM") != string::npos);
            cout << "[" << get_timestamp() << "] Using " 
                 << (use_ampm_format_ ? "AM/PM" : "24-hour") << " format" << endl;
        }
    }
    

    
    bool get_next_record(TrafficData& data) {
        string line;
        if (!getline(file_, line)) {
            return false;
        }
        
        vector<string> cols;
        cols.reserve(headers_.size());
        {
            string f;
            stringstream ss(line);
            while (getline(ss, f, ',')) {
                cols.push_back(f);
            }
        }
        
        // ID
        if (idx_id_ >= 0 && idx_id_ < (int)cols.size()) {
            try {
                data.id = stoi(cols[idx_id_]);
            } catch (...) {
                return false;
            }
        } else {
            return false;
        }
        
        // 許可されたID以外はスキップ
        if (!kAllowedIds.count(data.id)) {
            return true; // 次へ進むだけ
        }
        
        // 日時列（DATA_AS_OF）
        if (idx_ts_ >= 0 && idx_ts_ < (int)cols.size()) {
            try {
                tm timeinfo = {};
                string ts = cols[idx_ts_];
                stringstream time_ss(ts);
                
                // 検出されたフォーマットのみを使用（分岐なし）
                if (use_ampm_format_) {
                    if (!(time_ss >> get_time(&timeinfo, "%m/%d/%Y %I:%M:%S %p"))) {
                        data.timestamp = chrono::system_clock::now();
                    } else {
                        timeinfo.tm_isdst = -1;
                        time_t tt = mktime(&timeinfo);
                        if (tt == -1) {
                            data.timestamp = chrono::system_clock::now();
                        } else {
                            data.timestamp = chrono::system_clock::from_time_t(tt);
                        }
                    }
                } else {
                    if (!(time_ss >> get_time(&timeinfo, "%m/%d/%Y %H:%M:%S"))) {
                        data.timestamp = chrono::system_clock::now();
                    } else {
                        timeinfo.tm_isdst = -1;
                        time_t tt = mktime(&timeinfo);
                        if (tt == -1) {
                            data.timestamp = chrono::system_clock::now();
                        } else {
                            data.timestamp = chrono::system_clock::from_time_t(tt);
                        }
                    }
                }
            } catch (...) {
                data.timestamp = chrono::system_clock::now();
            }
        } else {
            data.timestamp = chrono::system_clock::now();
        }
        
        // 生CSV行をそのまま保存（JSON構築を避けて高速化）
        data.raw_csv_line = line;
        
        return true;
    }
};

// シグナルハンドラー
atomic<bool> global_quit{false};
void signal_handler(int sig) {
    global_quit.store(true);
}

// メイン関数
int main(int argc, char* argv[]) {
    cout << "[" << get_timestamp() << "] START: csv=" << (argc > 1 ? argv[1] : "none") << ", speed=1/" << SPEED_MULTIPLIER << endl;
    
    // 標準出力の最適化
    ios::sync_with_stdio(false);
    cin.tie(nullptr);
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    if (argc < 2) {
        cout << "Usage: " << argv[0] << " <csv_file>" << endl;
        return 1;
    }
    
    string csv_file = argv[1];
    std::unordered_map<int, std::unique_ptr<MqttClientManager>> clients;

    try {
        CsvProcessor csv_processor(csv_file);
        cout << "[" << get_timestamp() << "] CSV processor initialized" << endl;
        
        // 最初のレコードを読み込んでベース時間を設定
        TrafficData first_data;
        if (!csv_processor.get_next_record(first_data)) {
            cout << "[" << get_timestamp() << "] Failed to read first record" << endl;
            return 1;
        }
        
        // CSVの最初のレコードを「時刻ゼロ」として設定
        const auto t0_csv = first_data.timestamp;
        
       cout << "[" << get_timestamp() << "] Starting with ID: " << first_data.id 
        << ", CSV timestamp: " << chrono::duration_cast<chrono::seconds>(t0_csv.time_since_epoch()).count()
        << endl;

        
        // ブローカーURLの定義
        vector<string> broker_urls = {"tcp://10.20.22.172:1883", "tcp://10.20.22.173:1883"};
        
        // 許可IDを昇順に並べて、ブローカーにラウンドロビンで割り当て
        std::vector<int> allowed_ids_sorted(kAllowedIds.begin(), kAllowedIds.end());
        std::sort(allowed_ids_sorted.begin(), allowed_ids_sorted.end());
        
        std::unordered_map<int, std::string> id_to_broker;
        {
            size_t rr = 0;
            for (int id : allowed_ids_sorted) {
                id_to_broker[id] = broker_urls[rr];
                rr = (rr + 1) % broker_urls.size();
            }
        }
        
        // ID→クライアント のマップ（オンデマンド生成）
        clients.clear();
        clients.reserve(138); // 想定ID数
        
        cout << "[" << get_timestamp() << "] MQTT clients will be created on-demand" << endl;
        
        // クライアント作成関数
        auto create_client_for_id = [&](int id) -> MqttClientManager* {
            auto it = clients.find(id);
            if (it != clients.end()) {
                return it->second.get();
            }
            
            // 新しいクライアントを作成
            const string client_id = to_string(id);
            // 均等化済みの割り当てを使用（念のためフォールバックも用意）
            std::string broker = broker_urls[0];
            auto itb = id_to_broker.find(id);
            if (itb != id_to_broker.end()) broker = itb->second;
            
            cout << "[" << get_timestamp() << "] Creating client " << id << " with broker " << broker << endl;
            
            auto client = make_unique<MqttClientManager>(client_id, broker, [&, id](const string& uri) {
                // 再接続処理：RTT計測で最適なブローカーを選択
                cout << "[" << get_timestamp() << "] Reconnection requested for client " << id << " from " << uri << endl;
                
                // 現在のブローカーが失敗した場合、他のブローカーを試行
                vector<string> fallback_brokers;
                for (const auto& broker_url : broker_urls) {
                    if (broker_url != uri) {
                        fallback_brokers.push_back(broker_url);
                    }
                }
                
                if (!fallback_brokers.empty()) {
                    string best_fallback = select_best_broker(fallback_brokers);
                    if (!best_fallback.empty()) {
                        cout << "[" << get_timestamp() << "] Client " << id << " switching to fallback broker: " << best_fallback << endl;
                        
                        // 既存のクライアントを使用して再接続（削除・再作成は避ける）
                        auto it = clients.find(id);
                        if (it != clients.end()) {
                            try {
                                // 既存のクライアントで新しいブローカーに再接続
                                it->second->reconnect(best_fallback);
                                cout << "[" << get_timestamp() << "] Client " << id << " reconnecting to " << best_fallback << endl;
                            } catch (const std::exception& e) {
                                cout << "[" << get_timestamp() << "] Error during reconnect: " << e.what() << endl;
                            }
                        }
                    }
                } else {
                    // フォールバックブローカーがない場合は、元のブローカーに再接続
                    cout << "[" << get_timestamp() << "] Client " << id << " no fallback available, reconnecting to original broker" << endl;
                    
                    auto it = clients.find(id);
                    if (it != clients.end()) {
                        try {
                            it->second->reconnect(uri);
                        } catch (const std::exception& e) {
                            cout << "[" << get_timestamp() << "] Error during reconnect: " << e.what() << endl;
                        }
                    }
                }
            });
            
            auto* client_ptr = client.get();
            clients.emplace(id, move(client));
            
            client_ptr->start();
            
            cout << "[" << get_timestamp() << "] Client " << id << " created and started" << endl;
            
            return client_ptr;
        };
        
        cout << "[" << get_timestamp() << "] Ready to create clients on-demand" << endl;
        
        // 接続遅延対策：138 IDのクライアントを並列で一気に作成
        cout << "[" << get_timestamp() << "] Creating all 138 clients in parallel..." << endl;
        vector<thread> client_threads;
        client_threads.reserve(kAllowedIds.size());
        
        for (int id : kAllowedIds) {
            client_threads.emplace_back([&, id]() {
                create_client_for_id(id);
            });
        }
        
        // 全クライアントの作成完了を待機
        for (auto& th : client_threads) {
            th.join();
        }
        
        cout << "[" << get_timestamp() << "] All 138 clients created and connecting..." << endl;
        
        // ここで改めて壁時計の基準を取る（publisher開始時点）
        const auto t0_wall = std::chrono::steady_clock::now();
        cout << "[" << get_timestamp() << "] Publisher start time set: t0_wall = " 
             << chrono::duration_cast<chrono::milliseconds>(t0_wall.time_since_epoch()).count() << endl;
        
        cout << "[" << get_timestamp() << "] All clients ready for data transmission" << endl;
        
        // データ送信開始
        cout << "[" << get_timestamp() << "] Starting data transmission..." << endl;
        
        TrafficData data;
        size_t processed_count = 0;
        auto start_processing_time = chrono::steady_clock::now();
        
        while (!global_quit.load()) {
            if (!csv_processor.get_next_record(data)) {
                break;
            }

            // data.id のクライアントを取得または作成
            auto* cli = create_client_for_id(data.id);
            
            if (!cli || !cli->is_connected()) {
                // 未接続の場合はスキップ（固定スリープなし）
                static size_t skip_count = 0;
                skip_count++;
                
                // スキップ状況を定期的にログ出力
                if (skip_count % 10000 == 0) {
                    cout << "[" << get_timestamp() << "] CSV processing continues, but client " << data.id << " is not connected. Skipped " << skip_count << " records so far." << endl;
                }
                continue;
            }
            
            // 相対時間方式でdue_wallを計算
            auto delta = data.timestamp - t0_csv;           // CSV基準の相対時間
            auto scaled = delta / SPEED_MULTIPLIER;          // スピード倍率を適用
            auto due_time = t0_wall + scaled;                // 壁時計基準の送信予定時刻
            
            // 送信予定時刻まで待機（main側で時間制御）
            if (due_time > std::chrono::steady_clock::now()) {
                std::this_thread::sleep_until(due_time);
            }
            
            // メッセージを追加
            try {
                cli->add_message(data);
                processed_count++;
                
                // 進捗ログ（100,000件に1回）
                if (processed_count % 100000 == 0) {
                    auto now = chrono::steady_clock::now();
                    auto elapsed = chrono::duration_cast<chrono::seconds>(now - start_processing_time).count();
                    double rps = (elapsed > 0) ? (double)processed_count / elapsed : 0;
                    
                    // 接続状況を表示
                    int connected_count = 0;
                    for (const auto& kv : clients) {
                        if (kv.second) {
                            if (kv.second->is_connected()) connected_count++;
                        }
                    }
                    
                    cout << "[" << get_timestamp() << "] Progress: " << processed_count << " records, RPS: " << fixed << setprecision(1) << rps 
                         << ", Connected: " << connected_count << "/" << clients.size() << endl;
                }
                
            } catch (...) {
                // エラーは無視して次へ
                continue;
            }
        }
        
        // 処理完了
        auto total_processing_time = chrono::steady_clock::now() - start_processing_time;
        auto total_sec = chrono::duration_cast<chrono::seconds>(total_processing_time).count();
        
        // 全クライアントの削除された件数を合計
        size_t total_dropped = 0;
        for (const auto& kv : clients) {
            if (kv.second) {
                total_dropped += kv.second->get_dropped_count();
            }
        }
        
        cout << "[" << get_timestamp() << "] Done: records=" << processed_count 
             << ", elapsed=" << total_sec << " sec, rps=" 
             << (total_sec > 0 ? processed_count / total_sec : 0)
             << ", dropped=" << total_dropped << endl;
        
        // 全パブリッシャを強制切断
        cout << "[" << get_timestamp() << "] Force cleanup for all clients..." << endl;
        
        // 終了フラグを設定
        global_quit.store(true);
        
        // 全クライアント一括クリーンアップ
        for (auto& kv : clients) {
            if (kv.second) {
                kv.second->cleanup();
            }
        }
        
        clients.clear();
        
        cout << "[" << get_timestamp() << "] All clients cleanup completed" << endl;
        
    } catch (const exception& e) {
        cout << "[" << get_timestamp() << "] ERROR: " << e.what() << endl;
        
        try {
            global_quit.store(true);
            
            for (auto& kv : clients) {
                if (kv.second) {
                    kv.second->cleanup();
                }
            }
            clients.clear();
        } catch (...) {
            // クリーンアップエラーは無視
        }
        
        return 1;
    } catch (...) {
        cout << "[" << get_timestamp() << "] UNKNOWN ERROR" << endl;
        return 1;
    }
    
    return 0;
}
