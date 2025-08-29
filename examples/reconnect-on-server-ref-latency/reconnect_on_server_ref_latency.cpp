// reconnect_on_server_ref_latency.cpp
//
// This is a Paho MQTT C++ client, sample application.
//
// This application demonstrates an MQTT v5 publisher that handles server
// reference information for automatic reconnection to alternative brokers
// based on latency measurements.
//
// The sample demonstrates:
//  - MQTT v5 publisher with server reference handling
//  - Automatic reconnection to alternative brokers based on latency
//  - Ping functionality to measure broker latency
//  - Handling of USE_ANOTHER_SERVER (0x9c) and SERVER_MOVED (0x9d) reason codes
//  - Continuous publishing with automatic failover
//  - Simplified reconnection logic with complete client recreation
//

/*******************************************************************************
 * Copyright (c) 2024
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Initial implementation
 *******************************************************************************/

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
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <cfloat>

#include <mqtt/async_client.h>
#include <mqtt/connect_options.h>
#include <mqtt/properties.h>
#include <mqtt/reason_code.h>
#include "ping_utils.h"

using namespace std;

// Constants
const string TOPIC = "test/topic";
const string PAYLOAD = "Test message";
const int MAX_RECONNECT_ATTEMPTS = 3;
const int RECONNECT_DELAY_MS = 1000;
const int CONNECTION_TIMEOUT_MS = 5000;
const int PING_TIMEOUT_MS = 2000;

// Global variables for simple management
atomic<bool> quit{false};
atomic<int> message_counter{0};
string current_server_uri;
mqtt::async_client* current_client = nullptr;
thread* current_pub_thread = nullptr;

// ping_utilsインスタンス
ping_utils::ping_utils ping_utils_instance;

// Simple timestamp function
string get_timestamp() {
    auto now = chrono::system_clock::now();
    auto time_t = chrono::system_clock::to_time_t(now);
    auto ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;
    
    stringstream ss;
    ss << put_time(localtime(&time_t), "%H:%M:%S");
    ss << "." << setfill('0') << setw(3) << ms.count();
    return ss.str();
}

// Forward declarations
void delete_client();
void create_client(const string& new_server_uri);

// 共通の再接続処理を関数化
void reconnect_to_server(const string& server_uri) {
    try {
        if (current_client && current_client->is_connected()) {
            cout << "[" << get_timestamp() << "] Background thread: Sending DISCONNECT packet..." << endl;
            current_client->disconnect()->wait();  // MQTT DISCONNECT送信＋TCPクローズ待ち
            cout << "[" << get_timestamp() << "] Background thread: DISCONNECT packet sent successfully" << endl;
        }
    } catch (const exception& e) {
        cout << "[" << get_timestamp() << "] Background thread: WARNING: Error sending DISCONNECT: " << e.what() << endl;
    }
    
    // Pahoの内部処理終了
    if (current_client) {
        cout << "[" << get_timestamp() << "] Background thread: Stopping Paho internal processing..." << endl;
        current_client->stop_consuming();
    }
    
    delete_client();
    create_client(server_uri);
}

// サーバーの応答時間を測定する関数
struct ServerInfo {
    string address;
    int port;
    float ping_time; // ping応答時間（ミリ秒、小数点以下も含む）
    
    ServerInfo(const string& addr, int p = 1883) : address(addr), port(p), ping_time(FLT_MAX) {}
};

float ping_server(const string& address, int port = 1883) {
    try {
        // ping_utilsを使用してping測定
        double latency = ping_utils_instance.measure_ping_latency(address, PING_TIMEOUT_MS);
        if (latency < numeric_limits<double>::max()) {
            return static_cast<float>(latency);
        } else {
            return FLT_MAX;
        }
    } catch (const exception& e) {
        cout << "[" << get_timestamp() << "] Ping error for " << address << ":" << port << ": " << e.what() << endl;
        return FLT_MAX;
    }
}

// 最適なサーバーを選択する関数
ServerInfo select_best_server(const vector<string>& server_addresses) {
    vector<ServerInfo> servers;
    
    // 各サーバーの応答時間を測定
    cout << "[" << get_timestamp() << "] Measuring server response times..." << endl;
    for (const auto& addr : server_addresses) {
        string clean_addr = addr;
        // mqtt:// や tcp:// プレフィックスを除去
        if (clean_addr.rfind("mqtt://", 0) == 0) {
            clean_addr = clean_addr.substr(7);
        } else if (clean_addr.rfind("tcp://", 0) == 0) {
            clean_addr = clean_addr.substr(6);
        }
        
        // ポート番号を分離
        int port = 1883;
        size_t colon_pos = clean_addr.find(':');
        if (colon_pos != string::npos) {
            port = stoi(clean_addr.substr(colon_pos + 1));
            clean_addr = clean_addr.substr(0, colon_pos);
        }
        
        ServerInfo server(clean_addr, port);
        
        // 複数回pingを実行して平均値を取る
        const int ping_count = 3;
        vector<float> ping_times;
        
        for (int i = 0; i < ping_count; i++) {
            float ping_time = ping_server(clean_addr, port);
            if (ping_time < FLT_MAX) {
                ping_times.push_back(ping_time);
            }
            
            // 連続pingの間に少し待機
            if (i < ping_count - 1) {
                this_thread::sleep_for(chrono::milliseconds(100));
            }
        }
        
        // 成功したpingの平均値を計算
        if (!ping_times.empty()) {
            float total_time = 0;
            for (float time : ping_times) {
                total_time += time;
            }
            server.ping_time = total_time / ping_times.size();
            
            cout << "[" << get_timestamp() << "] Server " << clean_addr << ":" << port << " - Average: " << server.ping_time << "ms" << endl;
        } else {
            cout << "[" << get_timestamp() << "] Server " << clean_addr << ":" << port << " - unreachable" << endl;
        }
        
        servers.push_back(server);
    }
    
    // 応答時間が最も短いサーバーを選択
    auto best_server = min_element(servers.begin(), servers.end(), 
        [](const ServerInfo& a, const ServerInfo& b) {
            return a.ping_time < b.ping_time;
        });
    
    if (best_server != servers.end() && best_server->ping_time < FLT_MAX) {
        cout << "[" << get_timestamp() << "] Selected best server: " << best_server->address << ":" << best_server->port 
             << " (response time: " << best_server->ping_time << "ms)" << endl;
        return *best_server;
    } else {
        // すべてのサーバーが到達不能の場合、デフォルトサーバーを返す
        cout << "[" << get_timestamp() << "] All servers unreachable, using default: 10.20.22.169:1883" << endl;
        return ServerInfo("10.20.22.169", 1883);
    }
}

// Simple callback class
class simple_callback : public mqtt::callback {
private:
    function<void(const string&)> reconnect_func_;

public:
    simple_callback(function<void(const string&)> reconnect_func) 
        : reconnect_func_(reconnect_func) {}

    void connection_lost(const string& cause) override {
        cout << "[" << get_timestamp() << "] Connection lost: " << cause << endl;
        cout << "[" << get_timestamp() << "] Connection lost details - cause: '" << cause << "'" << endl;
        
        // 接続が失われた原因を分析
        if (cause.empty()) {
            cout << "[" << get_timestamp() << "] INFO: Connection lost with empty cause (likely network issue)" << endl;
        } else if (cause.find("timeout") != string::npos) {
            cout << "[" << get_timestamp() << "] INFO: Connection lost due to timeout" << endl;
        } else if (cause.find("error") != string::npos) {
            cout << "[" << get_timestamp() << "] INFO: Connection lost due to error: " << cause << endl;
        } else {
            cout << "[" << get_timestamp() << "] INFO: Connection lost due to: " << cause << endl;
        }
        
        // ★コールバックでは何もしない（再接続は別スレッドに移譲）
        cout << "[" << get_timestamp() << "] Deferring reconnection to background thread..." << endl;
        std::thread([cause]() {
            // 最適なサーバーを選択して再接続
            cout << "[" << get_timestamp() << "] Background thread: Starting reconnection process..." << endl;
            vector<string> default_servers = {"10.20.22.173:1883"};
            ServerInfo best_server = select_best_server(default_servers);
            
            string new_server_uri = "tcp://" + best_server.address + ":" + to_string(best_server.port);
            cout << "[" << get_timestamp() << "] Background thread: Reconnecting to best server: " << new_server_uri << endl;
            reconnect_to_server(new_server_uri);
        }).detach();
    }

    void setup_disconnected_handler(mqtt::async_client& client) {
        client.set_disconnected_handler([this](const mqtt::properties& props, const mqtt::ReasonCode& reason) {
            cout << "[" << get_timestamp() << "] Disconnected with reason code: 0x" << hex << static_cast<int>(reason) << dec << endl;
            cout << "[" << get_timestamp() << "] Disconnect reason details:" << endl;
            cout << "[" << get_timestamp() << "]   - Reason code: 0x" << hex << static_cast<int>(reason) << dec << " (" << static_cast<int>(reason) << ")" << endl;
            
            // プロパティの詳細を出力
            if (!props.empty()) {
                cout << "[" << get_timestamp() << "]   - Properties count: " << props.size() << endl;
                for (const auto& prop : props) {
                    cout << "[" << get_timestamp() << "]     Property: " << static_cast<int>(prop.type()) << endl;
                }
            } else {
                cout << "[" << get_timestamp() << "]   - No properties" << endl;
            }
            
            if (reason == mqtt::ReasonCode::USE_ANOTHER_SERVER || reason == mqtt::ReasonCode::SERVER_MOVED) {
                cout << "[" << get_timestamp() << "] Server reference detected, analyzing server list..." << endl;
                
                // サーバーリファレンスが返された場合、プロパティからサーバーリストを抽出
                vector<string> server_addresses;
                
                // プロパティからサーバーアドレスを抽出（軽い処理のみ）
                for (const auto& prop : props) {
                    if (prop.type() == mqtt::property::SERVER_REFERENCE) {
                        string server_ref;
                        try {
                            server_ref = get<string>(prop);
                            cout << "[" << get_timestamp() << "] Found server reference: " << server_ref << endl;
                        } catch (const exception& e) {
                            cout << "[" << get_timestamp() << "] WARNING: Error reading SERVER_REFERENCE property: " << e.what() << endl;
                            continue;
                        }
                        
                        // カンマ区切りのサーバーリストを解析
                        size_t pos = 0;
                        while (pos < server_ref.length()) {
                            size_t comma_pos = server_ref.find(',', pos);
                            if (comma_pos == string::npos) {
                                comma_pos = server_ref.length();
                            }
                            
                            string server = server_ref.substr(pos, comma_pos - pos);
                            // 空白を除去
                            server.erase(0, server.find_first_not_of(" \t"));
                            server.erase(server.find_last_not_of(" \t") + 1);
                            
                            if (!server.empty()) {
                                server_addresses.push_back(server);
                                cout << "[" << get_timestamp() << "] Added server: " << server << endl;
                            }
                            
                            pos = comma_pos + 1;
                        }
                    }
                }
                
                // サーバーリストが空の場合、デフォルトサーバーを追加
                if (server_addresses.empty()) {
                    cout << "[" << get_timestamp() << "] No server reference found, using default server" << endl;
                    server_addresses.push_back("10.20.22.173:1883");
                }
                
                // ★コールバックでは何もしない（再接続は別スレッドに移譲）
                cout << "[" << get_timestamp() << "] Deferring reconnection to background thread..." << endl;
                std::thread([server_addresses]() {
                    // 最適なサーバーを選択して再接続
                    ServerInfo best_server = select_best_server(server_addresses);
                    string new_server_uri = "tcp://" + best_server.address + ":" + to_string(best_server.port);
                    cout << "[" << get_timestamp() << "] Background thread: Reconnecting to best server: " << new_server_uri << endl;
                    reconnect_to_server(new_server_uri);
                }).detach();
                
            } else {
                // その他の理由で切断された場合、固定アドレスに再接続
                cout << "[" << get_timestamp() << "] Other disconnect reason, deferring reconnection to background thread..." << endl;
                
                // ★コールバックでは何もしない（再接続は別スレッドに移譲）
                std::thread([]() {
                    cout << "[" << get_timestamp() << "] Background thread: Reconnecting to default server..." << endl;
                    reconnect_to_server("mqtt://10.20.22.173:1883");
                }).detach();
            }
        });
    }
};

// グローバル寿命のコールバック（ダングリング参照を防ぐ）
std::shared_ptr<simple_callback> g_cb;

// 再試行カウンター（関数再入・多重接続の問題を防ぐ）
atomic<int> retry_count{0};

// コールバックをバインドする関数（ダングリング参照を防ぐ）
void bind_callback(mqtt::async_client& client) {
    if (!g_cb) {
        g_cb = std::make_shared<simple_callback>([&](const string& uri) {
            // 別スレッドに移譲してレースを防ぐ
            std::thread([uri]() {
                delete_client();
                create_client(uri);
            }).detach();
        });
    }
    client.set_callback(*g_cb);
    g_cb->setup_disconnected_handler(client);
}

// Simple publisher thread function
void publisher_thread(mqtt::async_client* client) {
    cout << "[" << get_timestamp() << "] Publisher thread started" << endl;
    
    while (!quit.load()) {
        try {
            if (client && client->is_connected()) {
                message_counter++;
                string topic = "test/topic";
                string payload = "Message #" + to_string(message_counter);
                
                cout << "[" << get_timestamp() << "] Publishing message #" << message_counter << " to topic: " << topic << endl;
                
                auto delivery_token = client->publish(topic, payload, 1, false);
                delivery_token->wait();
                
                if (delivery_token->get_return_code() == 0) {
                    cout << "[" << get_timestamp() << "] Message #" << message_counter << " published successfully" << endl;
                } else {
                    cout << "[" << get_timestamp() << "] Message #" << message_counter << " failed to publish" << endl;
                }
            } else {
                cout << "[" << get_timestamp() << "] Client not connected, waiting..." << endl;
            }
        } catch (const exception& e) {
            cout << "[" << get_timestamp() << "] ERROR in publisher thread: " << e.what() << endl;
        }
        
        this_thread::sleep_for(chrono::seconds(1));
    }
    
    cout << "[" << get_timestamp() << "] Publisher thread exiting..." << endl;
}

// Delete client completely
void delete_client() {
    cout << "[" << get_timestamp() << "] Deleting client..." << endl;
    
    // Step 1: Stop publisher thread
    quit.store(true);
    
    if (current_pub_thread && current_pub_thread->joinable()) {
        current_pub_thread->join();
    }
    
    // Step 2: Properly disconnect and delete client
    if (current_client) {
        try {
            // 接続状態を確認
            bool was_connected = current_client->is_connected();
            
            // 接続されている場合は適切なDISCONNECT、そうでない場合は強制DISCONNECT
            if (was_connected) {
                auto disconnect_token = current_client->disconnect();
                if (disconnect_token) {
                    disconnect_token->wait_for(chrono::milliseconds(5000));
                }
            } else {
                try {
                    current_client->disconnect();
                } catch (const exception& e) {
                    // エラーは無視（接続が失われている場合）
                }
            }
        } catch (const exception& e) {
            // エラーが発生しても処理を続行
        }
        
        try {
            delete current_client;
            current_client = nullptr;
        } catch (const exception& e) {
            cout << "[" << get_timestamp() << "] ERROR: Failed to delete client: " << e.what() << endl;
        }
    }
    
    // Step 3: Clean up thread pointer
    if (current_pub_thread) {
        delete current_pub_thread;
        current_pub_thread = nullptr;
    }
    
    // Step 4: Reset message counter
    message_counter = 0;
    
    cout << "[" << get_timestamp() << "] Client deletion completed" << endl;
}

// Create new client
void create_client(const string& new_server_uri) {
    cout << "[" << get_timestamp() << "] Creating new client..." << endl;
    
    try {
        // Step 1: Create new client with tcp:// scheme
        string client_id = "simple_client_" + to_string(chrono::duration_cast<chrono::milliseconds>(
            chrono::system_clock::now().time_since_epoch()).count());
        
        // URIスキームをtcp://に統一
        string uri = new_server_uri;
        if (uri.rfind("tcp://", 0) != 0 && uri.rfind("mqtt://", 0) != 0) {
            uri = "tcp://" + uri;
        } else if (uri.rfind("mqtt://", 0) == 0) {
            uri = "tcp://" + uri.substr(7); // mqtt://をtcp://に変換
        }
        
        current_client = new mqtt::async_client(uri, client_id);
        
        // Step 2: Set up callback using global callback
        bind_callback(*current_client);
        
        // Step 3: Connect with MQTT v5
        auto connect_options = mqtt::connect_options_builder()
            .mqtt_version(MQTTVERSION_5)  // MQTT v5を明示的に指定
            .clean_start(true)
            .connect_timeout(chrono::seconds(10))  // 秒単位で指定
            .keep_alive_interval(chrono::seconds(30))  // 秒単位で指定
            .finalize();
        
        auto connect_token = current_client->connect(connect_options);
        
        // 同期waitで安定化（内部でCONNECT送出が終わるまで待つ）
        connect_token->wait();  // タイムアウトなしで完了まで待機
        
        if (connect_token->get_return_code() == 0) {
            cout << "[" << get_timestamp() << "] SUCCESS: Connected to new server" << endl;
            
            // Step 4: Update global state
            current_server_uri = uri;
            
            // Step 5: Start new publisher thread
            quit.store(false);
            
            // パブリッシャースレッドを開始
            current_pub_thread = new thread(publisher_thread, current_client);
            
            cout << "[" << get_timestamp() << "] New client creation completed" << endl;
        } else {
            cout << "[" << get_timestamp() << "] FAILED: Connection failed with return code: " << connect_token->get_return_code() << endl;
            delete_client();
            throw runtime_error("Connection failed with return code: " + to_string(connect_token->get_return_code()));
        }
        
    } catch (const exception& e) {
        cout << "[" << get_timestamp() << "] ERROR: Failed to create client: " << e.what() << endl;
        delete_client();
        
        // 再試行（最大3回）
        if (retry_count.load() < 3) {
            retry_count.fetch_add(1);
            cout << "[" << get_timestamp() << "] Retry attempt " << retry_count.load() << "/3" << endl;
            cout << "[" << get_timestamp() << "] Waiting 5 seconds before retry..." << endl;
            this_thread::sleep_for(chrono::seconds(5));
            create_client(new_server_uri);
        } else {
            cout << "[" << get_timestamp() << "] Maximum retry attempts reached. Exiting..." << endl;
            quit.store(true);
        }
    }
}

int main(int argc, char* argv[]) {
    cout << "[" << get_timestamp() << "] === SIMPLE RECONNECT EXAMPLE START ===" << endl;
    
    // ping_utilsの初期化
    cout << "[" << get_timestamp() << "] Initializing ping utilities..." << endl;
    try {
        // ping_utilsは自動的に初期化されるため、特別な初期化は不要
        cout << "[" << get_timestamp() << "] Ping utilities ready" << endl;
    } catch (const exception& e) {
        cout << "[" << get_timestamp() << "] WARNING: Ping utilities initialization failed: " << e.what() << endl;
        cout << "[" << get_timestamp() << "] Continuing without ping functionality..." << endl;
    }
    
    // Pahoトレースを有効化して、CONNECT送出の有無を確認
    cout << "[" << get_timestamp() << "] Enabling Paho MQTT trace for debugging..." << endl;
    setenv("MQTT_C_CLIENT_TRACE", "ON", 1);
    setenv("MQTT_C_CLIENT_TRACE_LEVEL", "PROTOCOL", 1);
    
    // サーバーURIを設定
    string serverURI = "mqtt://10.20.22.169:1883";
    if (argc > 1) {
        serverURI = argv[1];
    }
    
    current_server_uri = serverURI;
    
    cout << "[" << get_timestamp() << "] Using server: " << serverURI << endl;
    
    // 10.20.22.169への接続テスト
    cout << "[" << get_timestamp() << "] Testing connection to 10.20.22.169:1883..." << endl;
    try {
        mqtt::async_client test_client("10.20.22.169:1883", "test_client");
        auto connOpts = mqtt::connect_options_builder()
            .clean_start(true)
            .connect_timeout(chrono::milliseconds(10000))  // 10秒に延長
            .finalize();
        
        auto conntok = test_client.connect(connOpts);
        if (conntok->wait_for(chrono::milliseconds(10000)) && conntok->get_return_code() == 0) {  // 10秒に延長
            cout << "[" << get_timestamp() << "] SUCCESS: 10.20.22.169:1883 is reachable" << endl;
            
            // 適切なDISCONNECTパケットを送信
            auto disconnect_token = test_client.disconnect();
            if (disconnect_token) {
                disconnect_token->wait_for(chrono::milliseconds(5000));
            }
        } else {
            cout << "[" << get_timestamp() << "] WARNING: 10.20.22.169:1883 connection test failed" << endl;
        }
    } catch (const exception& e) {
        cout << "[" << get_timestamp() << "] WARNING: 10.20.22.169:1883 connection test error: " << e.what() << endl;
    }
    
    // 初期クライアントを作成
    cout << "[" << get_timestamp() << "] Creating initial client..." << endl;
    
    // URIスキームをtcp://に統一
    string initial_uri = serverURI;
    if (initial_uri.rfind("tcp://", 0) != 0 && initial_uri.rfind("mqtt://", 0) != 0) {
        initial_uri = "tcp://" + initial_uri;
    } else if (initial_uri.rfind("mqtt://", 0) == 0) {
        initial_uri = "tcp://" + initial_uri.substr(7); // mqtt://をtcp://に変換
    }
    
    current_client = new mqtt::async_client(initial_uri, "simple_initial_client");
    
    // コールバックを設定（グローバルコールバックを使用）
    bind_callback(*current_client);
    
    // 接続（MQTT v5）
    cout << "[" << get_timestamp() << "] Connecting..." << endl;
    auto connect_options = mqtt::connect_options_builder()
        .mqtt_version(MQTTVERSION_5)  // MQTT v5を明示的に指定
        .clean_start(true)
        .connect_timeout(chrono::seconds(10))  // 秒単位で指定
        .keep_alive_interval(chrono::seconds(30))  // 秒単位で指定
        .finalize();
    
    auto connect_token = current_client->connect(connect_options);
    connect_token->wait();
    
    if (connect_token->get_return_code() == 0) {
        cout << "[" << get_timestamp() << "] SUCCESS: Connected to " << initial_uri << endl;
        
        // パブリッシャースレッドを開始
        quit.store(false);
        
        // パブリッシャースレッドを開始
        current_pub_thread = new thread(publisher_thread, current_client);
        
        cout << "[" << get_timestamp() << "] Publisher started. Press 'q' to quit." << endl;
        
        // メインループ
        string input;
        while (!quit.load()) {
            cout << "Enter 'q' to quit: ";
            getline(cin, input);
            if (input == "q") {
                quit.store(true);
                break;
            }
        }
        
        cout << "[" << get_timestamp() << "] Shutting down..." << endl;
        
        // クリーンアップ
        if (current_client) {
            try {
                if (current_client->is_connected()) {
                    auto disconnect_token = current_client->disconnect();
                    if (disconnect_token) {
                        disconnect_token->wait_for(chrono::milliseconds(5000));
                    }
                }
            } catch (const exception& e) {
                cout << "[" << get_timestamp() << "] WARNING: Error during final disconnect: " << e.what() << endl;
            }
            delete current_client;
            current_client = nullptr;
        }
        
    } else {
        cout << "[" << get_timestamp() << "] FAILED: Could not connect to " << initial_uri << endl;
        cout << "[" << get_timestamp() << "] Return code: " << connect_token->get_return_code() << endl;
        delete current_client;
        current_client = nullptr;
        return 1;
    }
    
    cout << "[" << get_timestamp() << "] === SIMPLE RECONNECT EXAMPLE END ===" << endl;
    return 0;
}
