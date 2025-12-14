#!/usr/bin/env bash
set -euo pipefail

# =============================================
# Настройки по умолчанию
# =============================================
REDUCERS=4
BLOCK_SIZE_KB=1024
CSV_DIR="csv_files_test"
JAR_BUILD_CMD="./gradlew clean jar"
HADOOP_MAIN_CLASS="Main"
HDFS_INPUT="/input"
HDFS_OUTPUT="/output"
HDFS_OUTPUT_TEMP="/output_temp"
RESULTS_FILE="results.txt"
DOCKER_COMPOSE="docker-compose"

# =============================================
# Цвета для логов (опционально)
# =============================================
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
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
# Функция: очистка HDFS
# =============================================
cleanup_hdfs() {
    log "Очистка HDFS..."
    docker exec namenode hdfs dfs -rm -r "$HDFS_INPUT" "$HDFS_OUTPUT" "$HDFS_OUTPUT_TEMP" 2>/dev/null || true
    docker exec namenode hdfs dfs -mkdir -p "$HDFS_INPUT"
}

# =============================================
# Функция: копирование CSV в HDFS
# =============================================
upload_csv_files() {
    if [[ ! -d "$CSV_DIR" ]]; then
        error "Директория $CSV_DIR не найдена!"
        exit 1
    fi

    local csv_files=("$CSV_DIR"/*.csv)
    if [[ ! -f "${csv_files[0]}" ]]; then
        error "CSV файлы не найдены в $CSV_DIR"
        exit 1
    fi

    log "Копирование CSV файлов в HDFS..."
    docker exec namenode rm -rf /tmp/*.csv || true

    for csv_file in "${csv_files[@]}"; do
        if [[ -f "$csv_file" ]]; then
            local filename=$(basename "$csv_file")
            log "Копирование $filename..."
            docker cp "$csv_file" namenode:/tmp/
            docker exec namenode hdfs dfs -put -f "/tmp/$filename" "$HDFS_INPUT/"
        fi
    done
}

# =============================================
# Функция: сборка и загрузка JAR
# =============================================
build_and_upload_jar() {
    log "Сборка проекта..."
    eval "$JAR_BUILD_CMD" || { error "Сборка не удалась"; exit 1; }

    local jar_file=$(find build/libs -name "*.jar" | head -n 1)
    if [[ ! -f "$jar_file" ]]; then
        error "JAR файл не найден после сборки"
        exit 1
    fi

    log "Копирование JAR в контейнер..."
    docker cp "$jar_file" namenode:/tmp/my-app.jar
}

# =============================================
# Функция: запуск MapReduce задания
# =============================================
run_mapreduce() {
    log "Запуск MapReduce задания на YARN..."
    docker exec namenode hadoop jar /tmp/my-app.jar "$HADOOP_MAIN_CLASS" \
        "$HDFS_INPUT" "$HDFS_OUTPUT" "$REDUCERS" "$BLOCK_SIZE_KB"
}

# =============================================
# Функция: получение результатов
# =============================================
fetch_results() {
    if docker exec namenode hdfs dfs -test -e "$HDFS_OUTPUT/_SUCCESS"; then
        log "Задание выполнено успешно!"
        echo "Category,Revenue,Quantity" > "$RESULTS_FILE"

        docker exec namenode hdfs dfs -ls "$HDFS_OUTPUT/part-r-*" 2>/dev/null | \
            awk '{print $NF}' | \
            while read file; do
                docker exec namenode hdfs dfs -cat "$file" >> "$RESULTS_FILE"
                echo "" >> "$RESULTS_FILE"
            done

        log "Результаты сохранены в $RESULTS_FILE"
    else
        error "Задание не завершилось успешно. Проверьте логи YARN."
        exit 1
    fi
}

# =============================================
# Функция: остановка кластера
# =============================================
stop_cluster() {
    read -rp "Остановить Hadoop кластер? (y/N): " stop_cluster
    if [[ "$stop_cluster" =~ ^[Yy]$ ]]; then
        log "Остановка кластера..."
        $DOCKER_COMPOSE down
    fi
}

# =============================================
# Основной поток выполнения
# =============================================
main() {
    log "Запуск MapReduce для анализа продаж"

    # Пользовательский ввод
    read -rp "Количество reducer-ов [$REDUCERS]: " user_reducers
    read -rp "Размер блока в KB [$BLOCK_SIZE_KB]: " user_block_size

    REDUCERS=${user_reducers:-$REDUCERS}
    BLOCK_SIZE_KB=${user_block_size:-$BLOCK_SIZE_KB}

    # Запуск кластера
    log "Запуск Hadoop кластера..."
    $DOCKER_COMPOSE up -d

    # Ожидание сервисов
    wait_for_service "http://localhost:9870" "NameNode"
    wait_for_service "http://localhost:9864" "DataNode"
    wait_for_service "http://localhost:8088" "ResourceManager"
    wait_for_service "http://localhost:8042" "NodeManager"

    # Очистка и подготовка
    cleanup_hdfs
    upload_csv_files
    build_and_upload_jar

    # Запуск задания
    run_mapreduce

    # Получение результатов
    fetch_results

    # Очистка временных файлов
    docker exec namenode rm -f /tmp/my-app.jar /tmp/*.csv || true

    # Остановка (по запросу)
    stop_cluster
}

# =============================================
# Обработка прерывания (Ctrl+C)
# =============================================
trap 'error "Скрипт прерван пользователем."; exit 130' INT

# Запуск
main "$@"