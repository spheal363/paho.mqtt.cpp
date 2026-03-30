// load_test_publisher.cpp
//
// MQTT負荷試験用Publisher
// - 300パブリッシャ（各ブローカー100個）
// - QoS 0、retain false、clean start
// - 16バイトペイロード（タイムスタンプ除く）
// - 1秒間隔でパブリッシュ
// - Ramp-Up 10秒（接続のみ）
// - パブリッシュ期間 700秒
// - Server Reference対応（RTT測定による最適ブローカー選択）

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
#include <mutex>
#include <csignal>
#include <climits>
#include <algorithm>
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

// 定数
const int PUBLISHERS_PER_BROKER = 100;
const int TOTAL_PUBLISHERS = 300;
const int RAMP_UP_SECONDS = 10;
const int PUBLISH_DURATION_SECONDS = 900;
const int PAYLOAD_SIZE = 16;  // タイムスタンプを除いた純粋なペイロードサイズ

// ブローカーURL
const vector<string> BROKER_URLS = {
    "tcp://10.20.22.172:1883",  // broker1
    "tcp://10.20.22.173:1883",  // broker2
    "tcp://10.20.22.168:1883"   // broker3
};

// ブローカーURLからトピック名を取得する関数
string get_topic_from_broker_uri(const string& uri) {
    if (uri.find("10.20.22.172") != string::npos) {
        return "test_data/1";
    } else if (uri.find("10.20.22.173") != string::npos) {
        return "test_data/2";
    } else if (uri.find("10.20.22.168") != string::npos) {
        return "test_data/3";
    }
    // デフォルト（通常は発生しない）
    return "test_data/1";
}

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

// URI正規化関数
static std::string normalize_tcp_uri(std::string s) {
    auto l = s.find_first_not_of(" \t");
    auto r = s.find_last_not_of(" \t");
    s = (l==std::string::npos) ? "" : s.substr(l, r-l+1);

    if (s.rfind("tcp://", 0) != 0) s = "tcp://" + s;

    std::string rest = s.substr(6);
    if (rest.find(':') == std::string::npos) {
        s += ":1883";
    }
    return s;
}

// 1回のTCP connect RTT測定
int measure_rtt_once(const string& host, int port, int timeout_ms) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        return -1;
    }
    
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags == -1) {
        close(sock);
        return -1;
    }
    if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1) {
        close(sock);
        return -1;
    }
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        struct addrinfo hints, *result;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        
        if (getaddrinfo(host.c_str(), nullptr, &hints, &result) != 0) {
            close(sock);
            return -1;
        }
        
        struct sockaddr_in* addr_in = (struct sockaddr_in*)result->ai_addr;
        addr.sin_addr = addr_in->sin_addr;
        freeaddrinfo(result);
    }
    
    auto start_time = std::chrono::steady_clock::now();
    int connect_result = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
    
    if (connect_result == 0) {
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        close(sock);
        return static_cast<int>(duration.count());
    }
    
    if (errno != EINPROGRESS) {
        close(sock);
        return -1;
    }
    
    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(sock, &write_fds);
    
    struct timeval timeout;
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;
    
    int select_result = select(sock + 1, nullptr, &write_fds, nullptr, &timeout);
    
    if (select_result <= 0) {
        close(sock);
        return -1;
    }
    
    int error = 0;
    socklen_t error_len = sizeof(error);
    if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, &error_len) == -1 || error != 0) {
        close(sock);
        return -1;
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    close(sock);
    return static_cast<int>(duration.count());
}

// TCP connect方式のRTT測定関数（3回繰り返し、平均値を返す）
int measure_rtt_ms(const string& host_or_uri, int timeout_ms = 1000) {
    string host = host_or_uri;
    int port = 1883;
    
    if (host.substr(0, 6) == "tcp://") {
        host = host.substr(6);
    }
    
    size_t colon_pos = host.find(':');
    if (colon_pos != string::npos) {
        string port_str = host.substr(colon_pos + 1);
        try {
            port = stoi(port_str);
        } catch (...) {
            port = 1883;
        }
        host = host.substr(0, colon_pos);
    }
    
    vector<int> successful_measurements;
    successful_measurements.reserve(3);
    
    for (int attempt = 0; attempt < 3; ++attempt) {
        int rtt = measure_rtt_once(host, port, timeout_ms);
        if (rtt >= 0) {
            successful_measurements.push_back(rtt);
        }
        
        if (attempt < 2) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    
    if (successful_measurements.empty()) {
        return timeout_ms;
    }
    
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
    string topic_;  // パブリッシュするトピック名
    mqtt::async_client* client;
    std::thread pub_thread;
    std::thread connect_thread;  // 接続用スレッド（追跡用）
    std::shared_ptr<MqttClientManager::ClientCallback> callback_;
    atomic<bool> quit{false};
    atomic<bool> connected{false};
    atomic<bool> publish_enabled{false};
    mutex client_mutex;
    
    // 再接続用のコールバック（fallbackブローカー選択用）
    function<void(const string&)> reconnect_func_;
    
    // 競合防止用フラグ
    std::atomic<bool> reconnecting_{false};
    
    // パブリッシュカウンタ
    std::atomic<size_t> published_count{0};
    
    // フォールバックブローカー選択用の関数ポインタ
    function<string(const vector<string>&)> select_best_broker_func_;
    
    // ブローカーURLリスト（フォールバック選択用）
    vector<string> broker_urls_;

public:
    MqttClientManager(const string& id, const string& uri, function<void(const string&)> reconnect_cb)
        : client_id(id), server_uri(uri), topic_(get_topic_from_broker_uri(uri)), client(nullptr), reconnect_func_(reconnect_cb) {}
    
    void set_select_best_broker_func(function<string(const vector<string>&)> func) {
        select_best_broker_func_ = func;
    }
    
    void set_broker_urls(const vector<string>& urls) {
        broker_urls_ = urls;
    }
    
    ~MqttClientManager() {
        if (quit.load()) return;
        cleanup();
    }
    
    size_t get_published_count() const {
        return published_count.load();
    }
    
    void cleanup(bool full = true) {
        connected.store(false);
        publish_enabled.store(false);
        quit.store(true);
        
        // パブリッシュスレッドの終了を待つ（タイムアウト付き）
        if (pub_thread.joinable()) {
            // 1秒待って終了しない場合は強制終了
            auto start = chrono::steady_clock::now();
            while (pub_thread.joinable() && 
                   (chrono::steady_clock::now() - start) < chrono::seconds(1)) {
                std::this_thread::sleep_for(chrono::milliseconds(100));
            }
            if (pub_thread.joinable()) {
                pub_thread.detach();  // タイムアウトした場合はデタッチ
            }
        }
        
        // 接続スレッドの終了を待つ（タイムアウト付き）
        if (connect_thread.joinable()) {
            auto start = chrono::steady_clock::now();
            while (connect_thread.joinable() && 
                   (chrono::steady_clock::now() - start) < chrono::seconds(1)) {
                std::this_thread::sleep_for(chrono::milliseconds(100));
            }
            if (connect_thread.joinable()) {
                connect_thread.detach();  // タイムアウトした場合はデタッチ
            }
        }
        
        // クライアントの切断（タイムアウト付き）
        if (client) {
            try {
                if (client->is_connected()) {
                    auto disconnect_token = client->disconnect();
                    if (disconnect_token) {
                        // 1秒でタイムアウト
                        disconnect_token->wait_for(chrono::seconds(1));
                    }
                }
                delete client;
                client = nullptr;
            } catch (...) {
                // エラー時は強制的に削除
                if (client) {
                    delete client;
                    client = nullptr;
                }
            }
        }
        
        if (!full) {
            quit.store(false);
            cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Reset flags for reconnection" << endl;
        }
    }
    
    bool is_connected() const {
        return connected.load();
    }
    
    void enable_publish() {
        publish_enabled.store(true);
    }
    
    void start() {
        // 接続を非同期で開始（ブロッキングしない）
        // スレッドを追跡するため、メンバ変数に保存
        if (connect_thread.joinable()) {
            connect_thread.detach();
        }
        connect_thread = std::thread([this]() {
            create_client();
        });
    }
    
    void reconnect(const string& new_uri) {
        if (reconnecting_.exchange(true)) {
            cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Reconnect already in progress. Skip." << endl;
            return;
        }
        
        cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Reconnection started" << endl;
        
        // 再接続前に publish_enabled の状態を保存
        bool was_publish_enabled = publish_enabled.load();
        
        {
            lock_guard<mutex> lock(client_mutex);
            server_uri = new_uri;
            topic_ = get_topic_from_broker_uri(new_uri);  // トピック名も更新
        }
        
        cleanup(false);
        create_client();
        
        // 再接続後に publish_enabled の状態を復元
        if (was_publish_enabled) {
            publish_enabled.store(true);
        }
        
        reconnecting_.store(false);
        
        cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Reconnected to " << new_uri << " (topic: " << topic_ << ", publish_enabled: " << (was_publish_enabled ? "true" : "false") << ")" << endl;
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
                .connect_timeout(chrono::seconds(10))
                .keep_alive_interval(chrono::seconds(15))
                .finalize();
            
            auto connect_token = client->connect(connect_options);
            connect_token->wait();
            
            if (connect_token->get_return_code() == 0) {
                cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Connected: " << server_uri << endl;
                connected.store(true);
                
                quit.store(false);
                pub_thread = std::thread(&MqttClientManager::publisher_thread, this);
                
            } else {
                cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Connect failed, rc=" << connect_token->get_return_code()
                     << " uri=" << server_uri << endl;
                delete client;
                client = nullptr;
                
                if (reconnect_func_) {
                    cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Attempting reconnection due to connect failure..." << endl;
                    reconnect_func_(server_uri);
                }
            }
        } catch (const std::exception& e) {
            cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - EXCEPTION in create_client: " << e.what()
                 << " uri=" << server_uri << endl;
            if (client) { delete client; client = nullptr; }
            
            if (reconnect_func_) {
                cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Attempting reconnection due to exception..." << endl;
                reconnect_func_(server_uri);
            }
        } catch (...) {
            cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - UNKNOWN EXCEPTION in create_client, uri=" << server_uri << endl;
            if (client) { delete client; client = nullptr; }
            
            if (reconnect_func_) {
                cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Attempting reconnection due to unknown exception..." << endl;
                reconnect_func_(server_uri);
            }
        }
    }
    
    void publisher_thread() {
        // 16バイトの固定ペイロード（タイムスタンプ除く）
        string base_payload(PAYLOAD_SIZE, 'X');
        
        // パブリッシュが有効になるまで待機
        while (!quit.load() && !publish_enabled.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        // パブリッシュが有効になったことをログ出力（最初の数件のみ）
        static atomic<int> publish_started_count{0};
        int count = publish_started_count.fetch_add(1);
        if (count < 5) {
            cout << "[" << get_timestamp() << "] Client ID: " << client_id << " - Publisher thread started, publish enabled" << endl;
        }
        
        while (!quit.load()) {
            if (!publish_enabled.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            
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
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            
            try {
                // 送信時刻を取得
                auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                
                // ペイロード: <unix_ms_timestamp>ts_sep_flag<16バイト固定データ>
                std::string payload = std::to_string(now_ms) + "ts_sep_flag" + base_payload;
                
                // クライアントとトピック名を取得（ロック内で一度に取得）
                mqtt::async_client* publish_client = nullptr;
                string current_topic;
                bool can_publish = false;
                
                {
                    lock_guard<mutex> client_lock(client_mutex);
                    
                    if (client && connected.load() && client->is_connected()) {
                        publish_client = client;
                        current_topic = topic_;
                        can_publish = true;
                    }
                }
                
                if (can_publish && publish_client) {
                    try {
                        // QoS 0, retain false（ブローカーごとのトピック名を使用）
                        // ユーザープロパティは付与しない
                        // 重要: std::stringを直接渡すと、ライブラリ内部でc_str()やstrlen()が
                        // 使われる可能性があるため、明示的にdata()とsize()を使用する
                        // これにより、0x00（NUL文字）が混入する問題を防ぐ
                        publish_client->publish(
                            current_topic, 
                            payload.data(),  // const void* として明示的に渡す
                            payload.size(),  // size_t として明示的にサイズを指定
                            0, 
                            false
                        );
                        published_count++;
                        
                        // 最初の数件のみログ出力
                        static atomic<int> first_publish_count{0};
                        int pub_count = first_publish_count.fetch_add(1);
                        if (pub_count < 5) {
                            cout << "[" << get_timestamp() << "] Client ID: " << client_id 
                                 << " - Published to " << current_topic << " (count: " << published_count.load() << ")" << endl;
                        }
                        
                    } catch (const std::exception& e) {
                        // エラーログを出力（最初の数件のみ）
                        static atomic<int> error_count{0};
                        int err_count = error_count.fetch_add(1);
                        if (err_count < 5) {
                            cout << "[" << get_timestamp() << "] Client ID: " << client_id 
                                 << " - Publish error: " << e.what() << endl;
                        }
                    } catch (...) {
                        // エラーは無視
                    }
                }
                
                // 1秒間隔でパブリッシュ
                std::this_thread::sleep_for(std::chrono::seconds(1));
                
            } catch (...) {
                // エラーは無視
            }
        }
    }
    
    // 内部コールバッククラス
    class ClientCallback : public mqtt::callback {
    private:
        MqttClientManager* manager;
        
    public:
        ClientCallback(MqttClientManager* mgr) : manager(mgr) {}
        
        void connection_lost(const string& cause) override {
            manager->connected.store(false);
            
            std::thread([this]() {
                cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Background thread: Connection lost, attempting reconnection..." << endl;
                
                string normalized_uri = normalize_tcp_uri(manager->server_uri);
                
                // フォールバックブローカーを選択して再接続
                if (manager->select_best_broker_func_ && !manager->broker_urls_.empty()) {
                    vector<string> fallback_brokers;
                    for (const auto& broker_url : manager->broker_urls_) {
                        if (broker_url != normalized_uri) {
                            fallback_brokers.push_back(broker_url);
                        }
                    }
                    
                    if (!fallback_brokers.empty()) {
                        string best_fallback = manager->select_best_broker_func_(fallback_brokers);
                        if (!best_fallback.empty()) {
                            cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Best fallback broker: " << best_fallback << endl;
                            manager->reconnect(best_fallback);
                            return;
                        }
                    }
                }
                
                // フォールバックがない場合、または選択関数がない場合は元のブローカーに再接続
                if (manager->reconnect_func_) {
                    manager->reconnect_func_(normalized_uri);
                } else {
                    manager->reconnect(normalized_uri);
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
                                server = normalize_tcp_uri(server);
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
                        
                        string best_server = select_best_broker(server_addresses, 2000);
                        
                        if (!best_server.empty()) {
                            cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Selected best server from reference: " << best_server << endl;
                            
                            std::thread([this, best_server]() {
                                cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Background thread: Starting reconnection to " << best_server << endl;
                                manager->reconnect(best_server);
                            }).detach();
                        } else {
                            cout << "[" << get_timestamp() << "] Client ID: " << manager->client_id << " - Failed to select best server from reference, using first one" << endl;
                            
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

// シグナルハンドラー
atomic<bool> global_quit{false};
atomic<bool> force_exit{false};

void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        global_quit.store(true);
        // 2回目のシグナルで強制終了
        static int signal_count = 0;
        signal_count++;
        if (signal_count >= 2) {
            force_exit.store(true);
            exit(1);
        }
    }
}

// メイン関数
int main(int argc, char* argv[]) {
    cout << "[" << get_timestamp() << "] START: MQTT Load Test Publisher" << endl;
    cout << "[" << get_timestamp() << "] Publishers: " << TOTAL_PUBLISHERS << " (" << PUBLISHERS_PER_BROKER << " per broker)" << endl;
    cout << "[" << get_timestamp() << "] Ramp-Up: " << RAMP_UP_SECONDS << " seconds" << endl;
    cout << "[" << get_timestamp() << "] Publish Duration: " << PUBLISH_DURATION_SECONDS << " seconds" << endl;
    cout << "[" << get_timestamp() << "] Payload Size: " << PAYLOAD_SIZE << " bytes (excluding timestamp)" << endl;
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    std::vector<std::unique_ptr<MqttClientManager>> clients;
    clients.reserve(TOTAL_PUBLISHERS);
    
    try {
        // クライアントID生成用カウンタ
        int client_counter = 0;
        
        // 各ブローカーに対して100個のクライアントを作成
        for (size_t broker_idx = 0; broker_idx < BROKER_URLS.size(); ++broker_idx) {
            const string& broker_uri = BROKER_URLS[broker_idx];
            
            for (int i = 0; i < PUBLISHERS_PER_BROKER; ++i) {
                string client_id = "load_test_" + to_string(client_counter++);
                
                // クライアントを作成
                auto client = make_unique<MqttClientManager>(client_id, broker_uri, [](const string& uri) {
                    // このコールバックは使用されない（connection_lost時に直接reconnectを呼び出す）
                });
                
                // フォールバックブローカー選択関数を設定
                client->set_select_best_broker_func([](const vector<string>& broker_urls) -> string {
                    return select_best_broker(broker_urls);
                });
                
                // ブローカーURLリストを設定
                client->set_broker_urls(BROKER_URLS);
                
                clients.push_back(move(client));
            }
        }
        
        cout << "[" << get_timestamp() << "] Created " << clients.size() << " clients" << endl;
        
        // Ramp-Up期間：10秒間で300クライアントを均等に接続
        cout << "[" << get_timestamp() << "] Starting Ramp-Up period (" << RAMP_UP_SECONDS << " seconds)..." << endl;
        
        auto ramp_up_start = chrono::steady_clock::now();
        auto ramp_up_interval = chrono::milliseconds((RAMP_UP_SECONDS * 1000) / TOTAL_PUBLISHERS);
        
        // 接続開始を並列化するため、各クライアントを別スレッドで起動
        vector<thread> start_threads;
        start_threads.reserve(clients.size());
        
        for (size_t i = 0; i < clients.size(); ++i) {
            if (global_quit.load()) break;
            
            // 均等に分散して接続開始（非同期）
            auto target_time = ramp_up_start + ramp_up_interval * i;
            start_threads.emplace_back([&clients, i, target_time]() {
                auto now = chrono::steady_clock::now();
                if (target_time > now) {
                    std::this_thread::sleep_until(target_time);
                }
                clients[i]->start();
            });
        }
        
        // 全スレッドの開始完了を待機
        for (auto& th : start_threads) {
            th.join();
        }
        
        // Ramp-Up完了まで待機（接続完了を確認する時間を確保）
        auto ramp_up_end = ramp_up_start + chrono::seconds(RAMP_UP_SECONDS);
        auto now = chrono::steady_clock::now();
        if (ramp_up_end > now) {
            std::this_thread::sleep_until(ramp_up_end);
        }
        
        // 接続完了を待つため、少し追加で待機
        std::this_thread::sleep_for(chrono::seconds(2));
        
        // 接続状況を確認
        int connected_count = 0;
        for (const auto& client : clients) {
            if (client && client->is_connected()) {
                connected_count++;
            }
        }
        
        cout << "[" << get_timestamp() << "] Ramp-Up completed. Connected: " << connected_count << "/" << clients.size() << endl;
        
        // パブリッシュ開始
        cout << "[" << get_timestamp() << "] Starting publish period (" << PUBLISH_DURATION_SECONDS << " seconds)..." << endl;
        
        // 全クライアントのパブリッシュを有効化
        cout << "[" << get_timestamp() << "] Enabling publish for all clients..." << endl;
        int enabled_count = 0;
        for (auto& client : clients) {
            if (client) {
                client->enable_publish();
                enabled_count++;
            }
        }
        cout << "[" << get_timestamp() << "] Enabled publish for " << enabled_count << " clients" << endl;
        
        // パブリッシュ期間を待機
        auto publish_start = chrono::steady_clock::now();
        auto publish_end = publish_start + chrono::seconds(PUBLISH_DURATION_SECONDS);
        
        while (!global_quit.load()) {
            now = chrono::steady_clock::now();
            if (now >= publish_end) {
                break;
            }
            // 100msごとにチェックして、即座に終了できるようにする
            std::this_thread::sleep_for(chrono::milliseconds(100));
        }
        
        cout << "[" << get_timestamp() << "] Publish period completed" << endl;
        
        // 統計情報を収集
        size_t total_published = 0;
        connected_count = 0;
        for (const auto& client : clients) {
            if (client) {
                total_published += client->get_published_count();
                if (client->is_connected()) {
                    connected_count++;
                }
            }
        }
        
        cout << "[" << get_timestamp() << "] Statistics: Published=" << total_published 
             << ", Connected=" << connected_count << "/" << clients.size() << endl;
        
        // 全クライアントを正常に切断
        cout << "[" << get_timestamp() << "] Disconnecting all clients..." << endl;
        global_quit.store(true);
        
        // クリーンアップを並列で実行（タイムアウト付き）
        vector<thread> cleanup_threads;
        cleanup_threads.reserve(clients.size());
        
        for (auto& client : clients) {
            if (client) {
                cleanup_threads.emplace_back([&client]() {
                    try {
                        client->cleanup();
                    } catch (...) {
                        // エラーは無視
                    }
                });
            }
        }
        
        // クリーンアップスレッドの完了を待つ（最大3秒）
        auto cleanup_start = chrono::steady_clock::now();
        for (auto& th : cleanup_threads) {
            if (force_exit.load()) {
                // 強制終了フラグが立っている場合は即座に終了
                break;
            }
            if (th.joinable()) {
                auto elapsed = chrono::steady_clock::now() - cleanup_start;
                if (elapsed < chrono::seconds(3)) {
                    th.join();
                } else {
                    th.detach();  // タイムアウトした場合はデタッチ
                }
            }
        }
        
        clients.clear();
        
        cout << "[" << get_timestamp() << "] All clients disconnected. Done." << endl;
        cout << "[" << get_timestamp() << "] ========================================" << endl;
        cout << "[" << get_timestamp() << "] トータルパブリッシュ数: " << total_published << endl;
        cout << "[" << get_timestamp() << "] ========================================" << endl;
        
    } catch (const exception& e) {
        cout << "[" << get_timestamp() << "] ERROR: " << e.what() << endl;
        
        // エラー時にも統計情報を収集して表示
        try {
            size_t total_published = 0;
            int connected_count = 0;
            for (const auto& client : clients) {
                if (client) {
                    total_published += client->get_published_count();
                    if (client->is_connected()) {
                        connected_count++;
                    }
                }
            }
            cout << "[" << get_timestamp() << "] ========================================" << endl;
            cout << "[" << get_timestamp() << "] トータルパブリッシュ数: " << total_published << endl;
            cout << "[" << get_timestamp() << "] ========================================" << endl;
        } catch (...) {
            // 統計情報の取得エラーは無視
        }
        
        try {
            global_quit.store(true);
            
            for (auto& client : clients) {
                if (client) {
                    client->cleanup();
                }
            }
            clients.clear();
        } catch (...) {
            // クリーンアップエラーは無視
        }
        
        return 1;
    } catch (...) {
        cout << "[" << get_timestamp() << "] UNKNOWN ERROR" << endl;
        
        // エラー時にも統計情報を収集して表示
        try {
            size_t total_published = 0;
            int connected_count = 0;
            for (const auto& client : clients) {
                if (client) {
                    total_published += client->get_published_count();
                    if (client->is_connected()) {
                        connected_count++;
                    }
                }
            }
            cout << "[" << get_timestamp() << "] ========================================" << endl;
            cout << "[" << get_timestamp() << "] トータルパブリッシュ数: " << total_published << endl;
            cout << "[" << get_timestamp() << "] ========================================" << endl;
        } catch (...) {
            // 統計情報の取得エラーは無視
        }
        
        return 1;
    }
    
    return 0;
}
