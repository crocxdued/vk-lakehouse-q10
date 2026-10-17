#!/usr/bin/env bash
# Обёртка над docker compose. Всё, кроме Docker, запускается в контейнерах.
#   ./run.sh all        — полный цикл: стек, медальон, проверки, бенчмарк, Spark, отчёт
#   ./run.sh <шаг>      — один шаг (список: ./run.sh help)
set -euo pipefail
cd "$(dirname "$0")"

tools() { docker compose run --rm tools "$@"; }

step() { echo; echo "================ $* ================"; }

case "${1:-help}" in
  up)
    docker compose up -d
    docker compose ps -a ;;
  build)
    docker compose build tools ;;
  status)
    docker compose ps -a
    echo; docker logs lakekeeper-init 2>&1 | tail -n 12
    echo; docker exec trino trino --execute "SHOW CATALOGS" ;;
  wait|medallion|check|partition|evolution|bench|spark-bench|spark-bronze|report|xlsx)
    tools "$1" ;;
  trino-cold)
    docker compose restart trino
    tools trino-cold ;;
  spark)
    # Trino на время Spark останавливаем, чтобы два JVM не делили память Docker
    docker compose stop trino
    tools spark-bench
    tools spark-bronze
    docker compose start trino ;;
  all)
    start=$(date +%s)
    step "стек";            docker compose up -d
    step "образ tools";     docker compose build tools
    docker compose ps -a > results/docker_ps.txt
    step "медальон";        tools medallion
    step "сверка с Q10";    tools check
    step "партиции";        tools partition
    step "evolution";       tools evolution
    step "бенчмарк Trino";  tools bench
    step "Trino после рестарта (кэш)"; docker compose restart trino; tools trino-cold
    step "Spark";           docker compose stop trino; tools spark-bench; tools spark-bronze; docker compose start trino
    step "отчёт";           tools report
    tools xlsx || true
    echo; echo "Готово за $(( ($(date +%s) - start) / 60 )) мин. Открой отчет.md и results/."
    ;;
  stop)
    docker compose stop ;;
  down)
    docker compose down -v ;;
  *)
    echo "Шаги: up | build | status | medallion | check | partition | evolution | bench |"
    echo "      trino-cold | spark | spark-bench | spark-bronze | report | xlsx | all | stop | down" ;;
esac
