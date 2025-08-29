#!/bin/bash

# CSVデータを使用したMQTTテストスクリプト
# reconnect_on_server_ref_latency_test.cppを使用してCSVデータをMQTTブローカーに送信

set -e

# スクリプトのディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# 設定
CSV_FILE="${1:-fullcol_1year.csv}"
BUILD_DIR="${PROJECT_ROOT}/build"
BIN_DIR="${BUILD_DIR}/examples/reconnect-on-server-ref-latency"
TEST_BINARY="${BIN_DIR}/reconnect_on_server_ref_latency_test"

# 色付きのログ出力
log_info() {
    echo -e "\033[32m[INFO]\033[0m $1"
}

log_warn() {
    echo -e "\033[33m[WARN]\033[0m $1"
}

log_error() {
    echo -e "\033[31m[ERROR]\033[0m $1"
}

log_step() {
    echo -e "\033[36m[STEP]\033[0m $1"
}

# ヘルプ表示
show_help() {
    echo "Usage: $0 [csv_file] [options]"
    echo ""
    echo "Arguments:"
    echo "  csv_file          CSVファイルのパス (default: sample.csv)"
    echo ""
    echo "Options:"
    echo "  -h, --help       このヘルプを表示"
    echo "  -c, --clean      ビルドディレクトリをクリーンアップ"
    echo "  -b, --build      プログラムをビルド"
    echo "  -r, --run        テストを実行"
    echo "  -f, --full       クリーン、ビルド、実行を順番に実行"
    echo ""
    echo "Examples:"
    echo "  $0                    # sample.csvを使用してテスト実行"
    echo "  $0 traffic_data.csv   # 指定したCSVファイルでテスト実行"
    echo "  $0 -f                 # フルビルド＆テスト実行"
    echo "  $0 -b                 # ビルドのみ実行"
}

# 依存関係チェック
check_dependencies() {
    log_step "依存関係をチェック中..."
    
    # 必要なコマンドの存在確認
    local missing_deps=()
    
    for cmd in cmake make g++ git; do
        if ! command -v $cmd &> /dev/null; then
            missing_deps+=($cmd)
        fi
    done
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        log_error "必要なコマンドが見つかりません: ${missing_deps[*]}"
        log_error "以下のパッケージをインストールしてください:"
        echo "  Ubuntu/Debian: sudo apt-get install cmake make g++ git"
        echo "  CentOS/RHEL: sudo yum install cmake make gcc-c++ git"
        exit 1
    fi
    
    log_info "依存関係チェック完了"
}

# ビルドディレクトリのクリーンアップ
clean_build() {
    log_step "ビルドディレクトリをクリーンアップ中..."
    
    if [ -d "$BUILD_DIR" ]; then
        # -fオプションの場合は、既存のbuildディレクトリを再利用
        if [ "$full_flag" = true ] && [ -f "$BUILD_DIR/CMakeCache.txt" ]; then
            log_info "既存のビルドディレクトリを再利用します: $BUILD_DIR"
            return 0
        fi
        
        rm -rf "$BUILD_DIR"
        log_info "ビルドディレクトリを削除しました: $BUILD_DIR"
    else
        log_info "ビルドディレクトリは存在しません"
    fi
}

# プログラムのビルド
build_program() {
    log_step "プログラムをビルド中..."
    
    # ビルドディレクトリを作成（既存の場合は再利用）
    if [ ! -d "$BUILD_DIR" ]; then
        mkdir -p "$BUILD_DIR"
    fi
    cd "$BUILD_DIR"
    
    # Gitサブモジュールの初期化確認
    cd "$PROJECT_ROOT"
    if [ ! -d "externals/paho-mqtt-c" ]; then
        log_info "Gitサブモジュールを初期化中..."
        git submodule update --init --recursive
    fi
    cd "$BUILD_DIR"
    
    # 既存のCMakeCache.txtがある場合は、CMake設定をスキップ
    if [ -f "CMakeCache.txt" ]; then
        log_info "既存のCMake設定を使用します"
    else
        # CMake設定
        log_info "CMake設定を実行中..."
        cmake .. \
            -DCMAKE_BUILD_TYPE=Release \
            -DPAHO_BUILD_STATIC=OFF \
            -DPAHO_BUILD_SHARED=ON \
            -DPAHO_BUILD_DOCUMENTATION=OFF \
            -DPAHO_BUILD_SAMPLES=ON \
            -DPAHO_BUILD_EXAMPLES=ON \
            -DPAHO_ENABLE_TESTING=OFF \
            -DPAHO_ENABLE_CPACK=OFF \
            -DPAHO_WITH_MQTT_C=ON \
            -DPAHO_WITH_SSL=OFF \
            -DCMAKE_PREFIX_PATH=/usr/local \
            -DCMAKE_MODULE_PATH=/usr/local/lib/cmake
    fi
    
    # ビルド実行
    log_info "ビルドを実行中..."
    make -j$(nproc) reconnect_on_server_ref_latency_test
    
    if [ -f "$TEST_BINARY" ]; then
        log_info "ビルド完了: $TEST_BINARY"
    else
        log_error "ビルドに失敗しました"
        exit 1
    fi
}

# CSVファイルの存在確認
check_csv_file() {
    log_step "CSVファイルをチェック中..."
    
    # スクリプトディレクトリからの相対パスを絶対パスに変換
    if [[ "$CSV_FILE" != /* ]]; then
        CSV_FILE="${SCRIPT_DIR}/${CSV_FILE}"
    fi
    
    if [ ! -f "$CSV_FILE" ]; then
        log_error "CSVファイルが見つかりません: $CSV_FILE"
        log_error "現在のディレクトリ: $(pwd)"
        log_error "スクリプトディレクトリ: $SCRIPT_DIR"
        log_error "利用可能なCSVファイル:"
        ls -la "${SCRIPT_DIR}"/*.csv 2>/dev/null || echo "  なし"
        exit 1
    fi
    
    # CSVファイルの内容を確認
    local line_count=$(wc -l < "$CSV_FILE")
    log_info "CSVファイル: $CSV_FILE (行数: $line_count)"
    
    # ヘッダー行を確認
    local header=$(head -1 "$CSV_FILE")
    log_info "ヘッダー: $header"
    
    # データ行のサンプルを表示
    local sample_data=$(head -2 "$CSV_FILE" | tail -1)
    log_info "サンプルデータ: $sample_data"
}

# テストの実行
run_test() {
    log_step "MQTTテストを実行中..."
    
    if [ ! -f "$TEST_BINARY" ]; then
        log_error "テストバイナリが見つかりません: $TEST_BINARY"
        log_error "先にビルドを実行してください (-b オプション)"
        exit 1
    fi
    
    # 実行権限を確認
    if [ ! -x "$TEST_BINARY" ]; then
        chmod +x "$TEST_BINARY"
    fi
    
    # 環境変数の設定（トレースOFFで高速化）
    unset MQTT_C_CLIENT_TRACE
    unset MQTT_C_CLIENT_TRACE_LEVEL
    
    log_info "テスト開始: $TEST_BINARY $CSV_FILE"
    log_info "MQTTブローカー: 10.20.22.172:1883, 10.20.22.173:1883"
    log_info "トピック: traffic/data"
    log_info "QoS: 1"
    
    # テスト実行
    "$TEST_BINARY" "$CSV_FILE"
    
    local exit_code=$?
    if [ $exit_code -eq 0 ]; then
        log_info "テスト完了"
    else
        log_error "テストが異常終了しました (終了コード: $exit_code)"
    fi
    
    return $exit_code
}

# メイン処理
main() {
    log_info "CSVデータMQTTテストスクリプト開始"
    log_info "プロジェクトルート: $PROJECT_ROOT"
    log_info "スクリプトディレクトリ: $SCRIPT_DIR"
    
    # オプション解析
    local clean_flag=false
    local build_flag=false
    local run_flag=false
    local full_flag=false
    
    # 最初にオプションを処理
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -c|--clean)
                clean_flag=true
                shift
                ;;
            -b|--build)
                build_flag=true
                shift
                ;;
            -r|--run)
                run_flag=true
                shift
                ;;
            -f|--full)
                full_flag=true
                shift
                ;;
            -*)
                log_error "不明なオプション: $1"
                show_help
                exit 1
                ;;
            *)
                # オプション以外の引数はCSVファイルとして扱う
                if [ -z "$CSV_FILE" ]; then
                    CSV_FILE="$1"
                fi
                shift
                ;;
        esac
    done
    
    # フルフラグが指定された場合
    if [ "$full_flag" = true ]; then
        clean_flag=true
        build_flag=true
        run_flag=true
    fi
    
    # オプションが指定されていない場合、デフォルトでビルドと実行
    if [ "$clean_flag" = false ] && [ "$build_flag" = false ] && [ "$run_flag" = false ]; then
        build_flag=true
        run_flag=true
    fi
    
    # 実行フラグが指定されている場合は、ビルドも必要
    if [ "$run_flag" = true ] && [ "$build_flag" = false ]; then
        build_flag=true
    fi
    
    # CSVファイルが指定されていない場合、デフォルトファイルを使用
    if [ -z "$CSV_FILE" ]; then
        CSV_FILE="sample.csv"
    fi
    
    # 依存関係チェック
    check_dependencies
    
    # Gitサブモジュールの初期化
    if [ ! -d "externals/paho-mqtt-c" ]; then
        log_step "Gitサブモジュールを初期化中..."
        git submodule update --init --recursive
    fi
    
    # クリーンアップ
    if [ "$clean_flag" = true ]; then
        clean_build
    fi
    
    # ビルド
    if [ "$build_flag" = true ]; then
        build_program
    fi
    
    # テスト実行
    if [ "$run_flag" = true ]; then
        check_csv_file
        run_test
    fi
    
    log_info "スクリプト完了"
}

# スクリプト実行
main "$@"

