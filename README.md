# Практическая №1 — локальный Lakehouse, вариант 2 (TPC-H Q10 «Returned Item Reporting»)

Стек: **MinIO** (S3) → **Lakekeeper** (Iceberg REST Catalog, метаданные в **PostgreSQL**) → **Trino** (SQL) + **Spark 3.5** (второй движок).
Медальон построен как в гайде 2: три слоя физически разделены — бакет MinIO, warehouse Lakekeeper и каталог Trino на каждый слой (`bronze`, `silver`, `gold`).

Отчёт по форме преподавателя — **`отчет.md`**. Он собирается из результатов замеров (`results/*.json`) командой `./run.sh report`. В конце отчёта — раздел 8 по лабораторной 2: строка для таблицы, метаданные, кэширование.

## Структура

```
.env                         ФИО, масштаб, кодеки, формат/кодек лабы 2 — все настройки здесь
docker-compose.yml           весь стек: minio, postgres, lakekeeper, trino + init-контейнеры, tools (Spark)
lakekeeper/warehouse-*.json  warehouse bronze/silver/gold поверх одноимённых бакетов
trino/catalog/               каталоги Trino: bronze, silver, gold (Iceberg REST) и tpch (генератор)
sql/00_schemas.sql           схемы lakehouse во всех трёх слоях
sql/01_bronze.sql            сырые customer, orders, lineitem, nation, region
sql/02_silver.sql            dim_customer (клиент + страна + регион), fct_returned_item (партиции по кварталу)
sql/03_gold.sql              top20_returned_customers_q4_1993 — ответ на Q10
sql/04_check.sql             сверка Gold с эталонным Q10
sql/05_evolution_timetravel.sql  часть 2: schema evolution и восстановление из снапшота
tools/Dockerfile             Python 3.12 + OpenJDK 17 + PySpark 3.5.6 + Iceberg 1.9.2 + клиент Trino
tools/lab.py                 оркестратор: медальон, проверки, бенчмарки Trino/Spark, evolution
tools/report.py              сборка отчет.md и заполнение строки таблицы лабы 2
run.sh                       обёртка: ./run.sh all | <шаг>
results/                     сырые замеры (json), лог Spark, docker ps
evidence/                    скриншоты (MinIO, Lakekeeper UI) — встраиваются в отчёт автоматически
```

## Запуск

Нужен только **Docker Desktop**. В Settings → Resources выдели **Memory ≥ 6 ГБ** (лучше 8).

1. Проверь `.env`: `FIO` — ровно как в VK Education, `GITHUB_REPO` — ссылка на свой репозиторий.
2. Полный цикл (~20–30 минут при sf1; первый раз дольше — скачиваются образы):
   ```bash
   chmod +x run.sh
   ./run.sh all
   ```
   Шаги по порядку: стек → образ `tools` → медальон → сверка с эталоном → замер партиций → schema evolution / time travel → бенчмарк Trino → Q10 после рестарта Trino → Spark (бенчмарк + чтение Bronze) → отчёт.
3. Открой `отчет.md` и проверь: в п. 4 сверка должна дать `0, 0`, нигде не должно остаться «нет данных».

Шаги можно запускать по одному: `./run.sh medallion`, `./run.sh bench`, `./run.sh spark`, `./run.sh report` (полный список — `./run.sh help`). Каждый шаг перезаписывает свой json в `results/`, отчёт пересобирается из того, что есть.

## Методология

**Данные.** Коннектор `tpch` генерирует TPC-H детерминированно: при одном `sfN` у всех одинаковые строки. Масштаб задаётся в `.env` (`SF`) и одинаков на всех слоях.

**Медальон.** Bronze — копия источника без изменений плюс `_ingested_at`. Silver — очистка (`trim`, нормализация регистра, отсев пустых ключей, дедупликация клиента), обогащение (страна и регион клиента) и факт возвращённых позиций с посчитанной выручкой, партиционированный по кварталу заказа. Gold — топ-20 клиентов за IV-1993. Корректность проверяется `EXCEPT` в обе стороны с эталонным Q10, выполненным прямо по `tpch`.

**Бенчмарк форматов и кодеков** (`./run.sh bench`). Для каждой пары из `COMBOS` четыре таблицы, нужные Q10, пишутся CTAS из `tpch` в схему `bronze.bench_<формат>_<кодек>`; кодек — сессионное свойство `bronze.compression_codec`. Время записи — серверное время запроса в Trino. Размер, число файлов и манифестов — из метатаблиц Iceberg `$files` / `$manifests`. Q10 выполняется `RUNS` раз; первый прогон учитывается отдельно, для остальных берётся медиана. Время и время планирования берутся из статистики Trino (`system.runtime.queries`).

**Метаданные** — тот же формат/кодек лабы 2, записанный с `target_max_file_size = 8MB`: данных столько же, файлов и манифестов больше, сравниваются время планирования и Q10.

**Кэширование** — Q10 сразу после записи, повторные прогоны, прогон после `docker compose restart trino` (холодные кэши метаданных и JIT). В Spark: первый прогон в новой сессии, повторные и прогон по `CACHE TABLE` с нужными колонками.

**Spark** читает те же таблицы через тот же REST-каталог (п. 6.2 отчёта). На время Spark-шагов Trino останавливается, чтобы два JVM не делили память Docker.

## Веб-интерфейсы

| Сервис | Адрес | Логин / пароль |
|---|---|---|
| MinIO Console | http://localhost:9001 | minio / minio12345 |
| Lakekeeper UI | http://localhost:8181/ui | — |
| Trino UI | http://localhost:8080 | любое имя, без пароля |

DBeaver: новое подключение Trino, `localhost:8080`, пользователь `trino`, пароль пустой. Таблицы пишутся трёхчастно: `bronze.lakehouse.lineitem`.

## Таблица лабы 2

`./run.sh report` печатает строку в отчёте (п. 8) и пишет её в `results/lab2_row.json`. Чтобы заполнить сам xlsx, положи таблицу в `private/VK_Лаба2.xlsx` и выполни `./run.sh xlsx`: результат появится в `private/VK_Лаба2_filled.xlsx`. Папка `private/` в git не попадает — в таблице данные других студентов.

## Остановка

```bash
./run.sh stop     # пауза, данные сохраняются
./run.sh down     # удалить контейнеры и данные
```

## Если что-то пошло не так

- **`port is already allocated`** — порт занят другим процессом: `lsof -nP -iTCP:9001 -sTCP:LISTEN`. Либо освободи порт, либо поменяй левую часть в `docker-compose.yml` (`"19001:9001"`).
- **`lakekeeper-init` пишет ошибку про warehouse.** При повторном `up` ответ «already exists» — это нормально. Иначе проверь, что бакеты созданы: `docker logs lakehouse-minio-init`.
- **Trino: `Catalog ... not found` или ошибки S3.** Убедись, что `lakekeeper-init` отработал до Trino, затем `docker compose restart trino`.
- **Trino падает или зависает на bench (память).** Увеличь Memory в Docker Desktop или поставь `SF=sf1`.
- **Spark: ошибки S3 или REST.** Посмотри вывод `./run.sh spark-bronze`. Lakekeeper и MinIO должны быть Up (`./run.sh status`).
- **Нужно начать с чистого листа:** `./run.sh down && ./run.sh all`.
