#!/usr/bin/env bash
set -euo pipefail

# =============================================
# Настройки
# =============================================
REDUCERS_LIST=(16 8 4 2 1)
BLOCK_SIZES_KB=(64 128 256 512 1024 2048 4096)
CSV_DIR="csv_files_test"
JAR_BUILD_CMD="./gradlew clean jar"
HADOOP_JAR_NAME="my-app.jar"
HADOOP_MAIN_CLASS="Main"
HDFS_INPUT="/input"
HDFS_OUTPUT="/output"
HDFS_OUTPUT_TEMP="/output_temp"
RESULTS_FILE="benchmark_results.csv"
DOCKER_COMPOSE="docker-compose"

# =============================================
# Цвета для логов
# =============================================
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()   { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# =============================================
# Функция: ожидание запуска сервиса
# =============================================
wait_for_service() {
    local url=$1
    local name=$2
    log "Ожидание запуска $name ($url)..."
    until curl -s --fail "$url" > /dev/null; do
        sleep 2
        echo -n "."
    done
    echo " Готово!"
}

# =============================================
# Функция: очистка и подготовка HDFS
# =============================================
prepare_hdfs_input() {
    log "Очистка и подготовка HDFS..."
    docker exec namenode hdfs dfs -rm -r "$HDFS_INPUT" "$HDFS_OUTPUT" "$HDFS_OUTPUT_TEMP" 2>/dev/null || true
    docker exec namenode hdfs dfs -mkdir -p "$HDFS_INPUT"

    if [[ ! -d "$CSV_DIR" ]]; then
        error "Директория $CSV_DIR не найдена!"
        exit 1
    fi

    for csv_file in "$CSV_DIR"/*.csv; do
        [[ -f "$csv_file" ]] || continue
        local filename=$(basename "$csv_file")
        log "Копирование $filename в HDFS..."
        docker cp "$csv_file" namenode:/tmp/
        docker exec namenode hdfs dfs -put -f "/tmp/$filename" "$HDFS_INPUT/" > /dev/null
    done
}

# =============================================
# Функция: сборка и загрузка JAR
# =============================================
build_and_upload_jar() {
    log "Сборка проекта..."
    eval "$JAR_BUILD_CMD"

    local jar_file
    jar_file=$(find build/libs -name "*.jar" | head -n 1)
    if [[ ! -f "$jar_file" ]]; then
        error "JAR файл не найден после сборки"
        exit 1
    fi

    log "Копирование JAR в контейнер..."
    docker cp "$jar_file" namenode:/tmp/"$HADOOP_JAR_NAME"
}

# =============================================
# Функция: запуск одного теста
# =============================================
run_test() {
    local reducers=$1
    local block_size=$2

    log "Тест: reducers=$reducers, block_size=${block_size}KB"

    # Очистка HDFS вывода
    docker exec namenode hdfs dfs -rm -r "$HDFS_OUTPUT" "$HDFS_OUTPUT_TEMP" 2>/dev/null || true

    local start_time
    start_time=$(date +%s)

    if docker exec namenode hadoop jar /tmp/"$HADOOP_JAR_NAME" "$HADOOP_MAIN_CLASS" "$HDFS_INPUT" "$HDFS_OUTPUT" "$reducers" "$block_size" > /tmp/hadoop_benchmark.log 2>&1; then
        local success=1
        if ! docker exec namenode hdfs dfs -test -e "$HDFS_OUTPUT/_SUCCESS" 2>/dev/null; then
            warn "_SUCCESS файл не найден"
            success=0
        fi
    else
        error "Ошибка выполнения MapReduce"
        success=0
    fi

    local end_time duration
    end_time=$(date +%s)
    duration=$((end_time - start_time))

    echo "$reducers,$block_size,$duration" >> "$RESULTS_FILE"

    if [[ $success -eq 1 ]]; then
        log "Успешно! Время: ${duration}s"
    else
        error "Неудача! Время: ${duration}s"
        tail -10 /tmp/hadoop_benchmark.log
    fi

    sleep 1
}

# =============================================
# Основной поток выполнения
# =============================================
main() {
    log "Запуск Hadoop кластера..."
    $DOCKER_COMPOSE up -d

    wait_for_service "http://localhost:9870" "NameNode"
    wait_for_service "http://localhost:9864" "DataNode"
    wait_for_service "http://localhost:8088" "ResourceManager"
    wait_for_service "http://localhost:8042" "NodeManager"

    build_and_upload_jar
    prepare_hdfs_input

    echo "reducers,block_size_kb,duration(seconds)" > "$RESULTS_FILE"

    for reducers in "${REDUCERS_LIST[@]}"; do
        for block_size in "${BLOCK_SIZES_KB[@]}"; do
            run_test "$reducers" "$block_size"
        done
    done

    log "Бенчмарки завершены. Результаты сохранены в $RESULTS_FILE"

    log "Очистка временных файлов..."
    docker exec namenode rm -f /tmp/*.csv /tmp/"$HADOOP_JAR_NAME" || true
}

trap 'error "Скрипт прерван пользователем."; exit 130' INT

main "$@"
