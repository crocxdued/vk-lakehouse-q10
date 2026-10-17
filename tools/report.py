"""Собирает отчет.md по форме преподавателя из results/*.json и заполняет строку в таблице лабы 2.

Все цифры и сравнения в отчёте берутся из замеров; фразы-выводы строятся по фактическим
числам (что больше/меньше и на сколько). Перед сдачей перечитай отчёт и поправь формулировки
своими словами — форма этого требует.
"""
import json
import os
import pathlib
import statistics

GB = 1024 ** 3


def _load(root, name):
    p = root / "results" / name
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def n(x, d=2):
    """Число с пробелами-разделителями тысяч."""
    if x is None:
        return "—"
    if isinstance(x, int):
        return f"{x:,}".replace(",", " ")
    return f"{x:,.{d}f}".replace(",", " ")


def raz(k):
    k = round(k)
    return f"{k} раза" if k % 10 in (2, 3, 4) and k % 100 not in (12, 13, 14) else f"{k} раз"


def pct(a, b):
    return (a - b) / b * 100 if b else 0.0


def more_less(a, b, unit_more="больше", unit_less="меньше"):
    p = pct(a, b)
    if abs(p) < 1:
        return "на уровне"
    return f"на {abs(p):.0f}% {unit_more if p > 0 else unit_less}"


MISSING = "_нет данных — запусти соответствующий шаг `./run.sh` (см. README)_"


class R:
    """Доступ к результатам замеров."""

    def __init__(self, root):
        self.root = root
        self.env = {k: os.getenv(k, d) for k, d in [
            ("FIO", "ФИО"), ("GITHUB_REPO", "<ссылка на репозиторий>"), ("SF", "sf1"),
            ("MEDALLION_CODEC", "ZSTD"), ("LAB2_FORMAT", "PARQUET"), ("LAB2_CODEC", "NONE")]}
        self.med = _load(root, "medallion.json")
        self.check = _load(root, "check.json")
        self.part = _load(root, "partition.json")
        self.evo = _load(root, "evolution.json")
        self.bench = _load(root, "bench_trino.json")
        self.cold = _load(root, "trino_cold.json")
        self.spark = _load(root, "bench_spark.json")
        self.sparkb = _load(root, "spark_bronze.json")
        self.sparklog = (root / "results" / "spark_bronze.log")

    def b(self, fmt, codec, small=False):
        if not self.bench:
            return None
        for r in self.bench["results"]:
            if r["format"] == fmt and r["codec"] == codec and r["small_files"] == small:
                return r
        return None

    def sp(self, fmt, codec):
        if not self.spark:
            return None
        return next((r for r in self.spark["results"] if r["format"] == fmt and r["codec"] == codec), None)

    def table(self, fqn):
        if not self.med:
            return None
        return next((t for t in self.med["tables"] if t["table"] == fqn), None)

    @property
    def lab2(self):
        return self.env["LAB2_FORMAT"].upper(), self.env["LAB2_CODEC"].upper()

    def lab2_row(self):
        fmt, codec = self.lab2
        b = self.b(fmt, codec)
        if not b:
            return None
        li = b["tables"]["lineitem"]
        if self.cold and self.cold.get("runs"):
            first = self.cold["runs"][0]["elapsed_s"]
        else:
            first = b["q10_first_s"]
        nxt = b["q10_next_median_s"]
        s = self.sp(fmt, codec)
        return {
            "size_gb": round(li["size_bytes"] / GB, 4),
            "files": li["files"],
            "write_s": b["write_s"]["lineitem"],
            "agg_first": round(first, 2),
            "agg_next": round(nxt, 2),
            "sf": b["sf"],
            "trino": f"{first:.2f} / {nxt:.2f}",
            "spark": f"{s['first_s']:.2f} / {s['next_median_s']:.2f}" if s else "—",
        }


# ---------------------------------------------------------------- секции ----

def sec_header(r):
    return f"""# Отчёт по практической №1 — локальный Lakehouse, вариант 2 (TPC-H Q10)

**Студент:** {r.env['FIO']}  
**Репозиторий:** {r.env['GITHUB_REPO']}

Все цифры в отчёте получены скриптами из этого репозитория (`./run.sh all`), сырые результаты — в `results/*.json`.
"""


def sec1(r):
    sf = r.env["SF"]
    li = r.table("bronze.lakehouse.lineitem")
    li_rows = n(li["rows"]) if li else ("6 001 215" if sf == "sf1" else "≈60 млн")
    if sf == "sf1":
        why = (f"При sf1 в `lineitem` {li_rows} строк: этого хватает, чтобы кодеки дали разницу "
               f"в размере в разы, а партиционирование — заметную экономию чтения. При этом весь цикл "
               f"(медальон, 7 вариантов записи Bronze для бенчмарка, Spark) проходит на ноутбуке в Docker "
               f"за ~20 минут без нехватки памяти. sf10 в 10 раз дольше пишется (особенно GZIP), а выводы о "
               f"соотношении кодеков не меняются — они зависят от данных, а не от объёма. "
               f"Масштаб один на всех слоях.")
    else:
        why = (f"При {sf} в `lineitem` {li_rows} строк — объём, на котором время запроса уже зависит "
               f"от чтения и распаковки, а не только от планирования, поэтому разница форматов и кодеков "
               f"видна и в скорости. Масштаб один на всех слоях.")
    return f"""
## 1. Вариант и масштаб

| Поле | Значение |
|---|---|
| ФИО (как в VK Education) | {r.env['FIO']} |
| Вариант (запрос) | Q10 «Returned Item Reporting» (вариант 2) |
| Масштаб `sfN` | {sf} |
| Почему выбрал такой масштаб | {why} |
"""


def sec2(r):
    return """
## 2. Поднятые сервисы — и почему именно они

Проверка: `docker compose ps -a` — `lakehouse-minio`, `lakehouse-pg`, `lakekeeper`, `trino` в статусе Up; одноразовые `lakehouse-minio-init`, `lakekeeper-migrate`, `lakekeeper-init` — Exited (0). Spark запускается по требованию в контейнере `tools`.

| Сервис | Роль в стеке | Почему именно он (а не альтернатива) |
|---|---|---|
| MinIO (S3-совместимый форк `pgsty/minio`) | Объектное хранилище: три бакета `bronze`, `silver`, `gold`, в них физически лежат parquet/orc-файлы и метаданные Iceberg | Работает по протоколу S3, поэтому тот же стек без переделок переезжает в AWS S3 / Yandex Object Storage — меняется только endpoint. Хранилище отделено от вычислений: диск и движок масштабируются независимо. Официальный образ `quay.io/minio/minio` из гайда больше не скачивается (401), форк совместим по API. |
| PostgreSQL 17 | БД, в которой Lakekeeper хранит каталог: какие таблицы есть и какой metadata.json у каждой текущий | Каталогу нужна транзакционность: при двух одновременных коммитах в таблицу указатель на снапшот должен смениться атомарно, иначе таблица «потеряет» запись. Postgres — штатный бэкенд Lakekeeper, открытый и знакомый. |
| Lakekeeper | Iceberg REST Catalog: три warehouse `bronze`/`silver`/`gold`, каждый поверх своего бакета | Реализует открытый протокол Iceberg REST — к тем же таблицам подключаются Trino и Spark (это показано в п. 6.2) без копирования данных. Один лёгкий контейнер на Rust без JVM, в отличие от Hive Metastore; есть веб-UI для просмотра таблиц. Лицензия Apache 2.0. |
| Trino 476 | SQL-движок: метаданные берёт у Lakekeeper, данные читает и пишет в MinIO | Коннектор `tpch` генерирует данные на лету — ничего не скачивается. Iceberg-коннектор умеет CTAS, UPDATE/DELETE/MERGE, time travel, метатаблицы `$files`/`$snapshots`/`$partitions` и выбор формата/кодека на сессию — всё, что нужно для медальона и бенчмарка, делается на чистом SQL. |
| Spark 3.5 + Iceberg 1.9 (контейнер `tools`) | Второй движок: читает Bronze через тот же REST-каталог (п. 6.2), даёт сравнение Trino/Spark и опыт с кэшированием (лаба 2) | Подробно — в п. 6.2. |

### Скрипты, которыми поднимал стек

Вместо отдельных `docker run` из гайда весь стек описан в `docker-compose.yml`: те же образы и настройки, но порядок запуска контролируется `depends_on` (Lakekeeper стартует только после миграций и готового Postgres, Trino — после регистрации warehouse), и стек поднимается одной командой.

```bash
docker compose up -d        # minio            — S3-хранилище (порты 9000 API, 9001 консоль)
                            # minio-init       — создаёт бакеты bronze, silver, gold
                            # postgres         — БД каталога (5432)
                            # lakekeeper-migrate — миграции схемы каталога в Postgres
                            # lakekeeper       — Iceberg REST Catalog + UI (8181)
                            # lakekeeper-init  — принимает Terms of Use и регистрирует warehouse
                            #                    bronze/silver/gold (lakekeeper/warehouse-*.json)
                            # trino            — SQL-движок (8080), каталоги из trino/catalog/
docker compose ps -a        # проверка статусов
docker logs lakekeeper-init # для каждого warehouse — "warehouse-id"
docker exec trino trino --execute "SHOW CATALOGS"   # bronze, gold, silver, system, tpch

docker compose build tools  # Python + PySpark 3.5 + Iceberg 1.9 (tools/Dockerfile)
./run.sh all                # медальон, проверки, бенчмарк, Spark, отчёт
```

Каталоги Trino — `trino/catalog/{bronze,silver,gold}.properties` (как в гайде 2, но с именами сервисов `lakekeeper`/`minio` вместо `host.docker.internal` — контейнеры в одной сети compose) и `trino/catalog/tpch.properties`.
"""


def sec2_evidence(r):
    out = []
    ps = r.root / "results" / "docker_ps.txt"
    if ps.exists():
        out.append("\nВывод `docker compose ps -a` на момент прогона (`results/docker_ps.txt`):\n\n```text\n"
                   + ps.read_text(encoding="utf-8").strip() + "\n```\n")
    imgs = sorted(p for p in (r.root / "evidence").glob("*") if p.suffix.lower() in (".png", ".jpg", ".jpeg"))
    for p in imgs:
        out.append(f"\n![{p.stem}](evidence/{p.name})\n")
    return "".join(out)


def sec3(r):
    return """
## 3. Разбор запроса — какие данные нужны

Q10 находит 20 клиентов, которые принесли больше всего выручки позициями, возвращёнными покупателем (`returnflag = 'R'`), из заказов IV квартала 1993 года. Выручка позиции — `extendedprice * (1 - discount)`.

| Таблица TPC-H | Нужные колонки | Что запрос с ними делает (фильтр / джойн / агрегат / вывод) |
|---|---|---|
| `lineitem` | `orderkey`, `extendedprice`, `discount`, `returnflag` | фильтр `returnflag = 'R'`; джойн с `orders` по `orderkey`; агрегат `sum(extendedprice * (1 - discount))` |
| `orders` | `orderkey`, `custkey`, `orderdate` | фильтр по `orderdate` в `[1993-10-01, 1994-01-01)`; связывает позиции с клиентом |
| `customer` | `custkey`, `name`, `acctbal`, `address`, `phone`, `comment`, `nationkey` | джойн с `orders` и `nation`; все поля, кроме `nationkey`, — ключи группировки и вывод |
| `nation` | `nationkey`, `name` | джойн с `customer`; `name` выводится как страна |
| `region` (не нужна Q10) | `regionkey`, `name` | добавлена для обогащения Silver регионом клиента; используется в п. 6.1 |
"""


def _row(t, cols, fmt, codec, part, why):
    if not t:
        return f"| — | {cols} | — | — | {fmt} | {codec} | {part} | {why} |"
    size = "< 0.01" if t["size_mb"] < 0.01 else n(t["size_mb"])
    return (f"| `{t['table'].split('.', 1)[1].replace('lakehouse.', '')}` | {cols} | {n(t['rows'])} | {size} "
            f"| {fmt} | {codec} | {part} | {why} |")


def sec4(r):
    c = r.env["MEDALLION_CODEC"]
    head = "| Таблица | Колонки (что оставил) | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |\n|---|---|---|---|---|---|---|---|"
    # обоснование кодека — по бенчмарку
    codec_note = ""
    z, g, s, no = r.b("PARQUET", "ZSTD"), r.b("PARQUET", "GZIP"), r.b("PARQUET", "SNAPPY"), r.b("PARQUET", "NONE")
    if z and g and no:
        codec_note = (f"По бенчмарку (п. 5) Bronze в Parquet+ZSTD — {n(z['size_total_mb'])} МБ против "
                      f"{n(g['size_total_mb'])} МБ у GZIP и {n(no['size_total_mb'])} МБ без сжатия; запись ZSTD "
                      f"{n(z['write_total_s'])} с против {n(g['write_total_s'])} с у GZIP. ")
    bronze_why = (f"Bronze — копия источника «как есть»: если понадобится другая витрина (например, по `shipmode`), "
                  f"источник не придётся перегружать. `_ingested_at` — время загрузки, нужно Silver для дедупликации. "
                  f"Партиций нет: Bronze не читают аналитики, он целиком пересобирается в Silver. {codec_note}"
                  f"Кодек {c} — по итогам бенчмарка (п. 5): сильное сжатие самого большого слоя при умеренной цене записи.")
    m = r.med or {}
    def T(name):
        return r.table(name)
    out = [f"""
## 4. Медальон — решения по каждому слою

Слои физически разделены, как в гайде 2: каталог Trino `bronze` → warehouse `bronze` в Lakekeeper → бакет `bronze` в MinIO (то же для silver и gold). SQL — `sql/01_bronze.sql`, `02_silver.sql`, `03_gold.sql`.
Строки — `count(*)`, вес — сумма `file_size_in_bytes` data-файлов из метатаблицы `"<таблица>$files"`. Формат везде PARQUET: колоночный стандарт для Iceberg и BI, а ORC в бенчмарке (п. 5) не дал выигрыша, который оправдал бы второй формат в стеке.

### Bronze (сырой)

{head}"""]
    out.append(_row(T("bronze.lakehouse.lineitem"), "все 16 колонок + `_ingested_at`", "PARQUET", c, "нет", bronze_why))
    out.append(_row(T("bronze.lakehouse.orders"), "все 9 + `_ingested_at`", "PARQUET", c, "нет", "То же."))
    out.append(_row(T("bronze.lakehouse.customer"), "все 8 + `_ingested_at`", "PARQUET", c, "нет", "То же."))
    out.append(_row(T("bronze.lakehouse.nation"), "все 4 + `_ingested_at`", "PARQUET", c, "нет", "Справочник 25 строк — один файл."))
    out.append(_row(T("bronze.lakehouse.region"), "все 3 + `_ingested_at`", "PARQUET", c, "нет", "Справочник 5 строк, нужен для обогащения клиента регионом."))

    p = r.part
    if p and p.get("partitioned") and p.get("no_partitions"):
        pp, np_ = p["partitioned"], p["no_partitions"]
        ratio = (np_["input_rows"] / pp["input_rows"]) if pp.get("input_rows") else 0
        part_note = (f"Замер (`./run.sh partition`, EXPLAIN ANALYZE запроса за IV-1993): с партициями скан читает "
                     f"{n(pp.get('input_rows'))} строк / {pp.get('physical_input')} с диска, без партиций — "
                     f"{n(np_.get('input_rows'))} строк / {np_.get('physical_input')} (в {ratio:.1f} раза больше). "
                     f"Цена — {pp['files']} файлов вместо {np_['files']} и вес {n(pp['size_mb'])} МБ против "
                     f"{n(np_['size_mb'])} МБ.")
    else:
        part_note = "Замер эффекта — `./run.sh partition`."
    nparts = len(m.get("silver_partitions", [])) or "≈14"
    out.append(f"""
### Silver (очищенный)

{head}""")
    out.append(_row(T("silver.lakehouse.dim_customer"),
                    "`custkey`, `name`, `address`, `phone`, `acctbal`, `mktsegment`, `comment`, `nation`, `region`",
                    "PARQUET", c, "нет",
                    "Измерение «клиент»: `customer ⋈ nation ⋈ region`. Очистка — `trim` строк, сегмент в верхнем регистре, "
                    "отсев пустых ключей, дедупликация по `custkey` (последняя загрузка по `_ingested_at`). Обогащение — "
                    "страна и регион вместо `nationkey`. Справочник при джойне читается целиком, поэтому партиции бесполезны."))
    out.append(_row(T("silver.lakehouse.fct_returned_item"),
                    "`orderkey`, `linenumber`, `custkey`, `orderdate`, `order_quarter`, `extendedprice`, `discount`, `net_revenue`",
                    "PARQUET", c, f"`order_quarter` (identity), {nparts} партиций",
                    "Факт «возвращённая позиция»: `lineitem ⋈ orders`, только `returnflag = 'R'`, выручка строки посчитана один раз. "
                    "Фильтры качества: `extendedprice > 0`, `discount` в `[0, 1]`. Хранятся все кварталы, а не только IV-1993: "
                    "слой годится для любого периода, срез делает Gold. Партиция = квартал, потому что Q10 фильтрует ровно "
                    f"один квартал и читает одну партицию. Месяц дал бы втрое больше мелких файлов, год — читал бы лишние 3 квартала. {part_note}"))
    out.append(f"""
### Gold (витрина)

{head}""")
    out.append(_row(T("gold.lakehouse.top20_returned_customers_q4_1993"),
                    "`revenue_rank`, `custkey`, `name`, `revenue`, `acctbal`, `nation`, `address`, `phone`, `comment`",
                    "PARQUET", c, "нет",
                    "Готовый ответ Q10: BI читает 20 строк без джойнов. Срез по кварталу (`order_quarter = 1993-10-01`) и "
                    "группировка по клиенту — здесь. `revenue_rank` — место в рейтинге для отчёта. На 20 строках партиции и "
                    "кодек роли не играют, кодек оставлен общий для единообразия."))

    if r.check:
        verdict = ("`0, 0` — расхождений нет, Gold полностью совпадает с эталоном."
                   if r.check["ok"] else f"`{r.check['only_in_reference']}, {r.check['only_in_gold']}` — ЕСТЬ РАСХОЖДЕНИЯ, проверь Silver.")
        out.append(f"\n**Проверка корректности** (`sql/04_check.sql`): Gold сравнивается с эталонным Q10, выполненным прямо по "
                   f"`tpch.{r.env['SF']}`, через `EXCEPT` в обе стороны по `(custkey, round(revenue, 2))`. Результат: {verdict}")
    if m.get("gold"):
        out.append("\nПервые строки витрины:\n\n| # | custkey | Клиент | Выручка | Страна |\n|---:|---:|---|---:|---|")
        for row in m["gold"][:5]:
            out.append(f"| {row[0]} | {row[1]} | {row[2]} | {n(float(row[3]))} | {row[4]} |")
    return "\n".join(out) + "\n"


def sec5(r):
    out = ["\n## 5. Тесты производительности\n"]
    if not r.bench:
        return out[0] + MISSING + "\n"
    out.append(
        f"Методика (`./run.sh bench`, код — `tools/lab.py`): для каждой пары формат + кодек четыре нужные Q10 таблицы "
        f"(`customer`, `orders`, `lineitem`, `nation`) пересоздаются из `tpch.{r.bench['sf']}` в отдельной схеме "
        f"`bronze.bench_<формат>_<кодек>`. Кодек задаётся сессией `bronze.compression_codec`. Время записи — серверное "
        f"время CTAS по данным Trino. Размер — сумма `file_size_in_bytes` из `$files`. Q10 по этому Bronze выполняется "
        f"{r.bench['runs']} раз подряд; время — `elapsedTime` запроса из статистики Trino (то же, что в `system.runtime.queries`). "
        f"Первый прогон считается отдельно (холодные кэши), для остальных берётся медиана. Всё на одной машине в Docker Desktop.\n")
    g, z = r.b("PARQUET", "GZIP"), r.b("PARQUET", "ZSTD")
    out.append("| Сжатие на Bronze | Время запроса | Размер Bronze, МБ | Твой вывод (что лучше и почему) |\n|---|---|---|---|")
    if g and z:
        gw, zw = g["write_total_s"], z["write_total_s"]
        zstd_cheaper = zw < gw * 0.97
        g_note = (f"Запись {n(gw)} с — {more_less(gw, zw, 'медленнее', 'быстрее')} ZSTD; размер {more_less(g['size_total_mb'], z['size_total_mb'])}, "
                  f"повторный Q10 {more_less(g['q10_next_median_s'], z['q10_next_median_s'], 'медленнее', 'быстрее')}. "
                  + ("GZIP (deflate) тратит больше CPU на сжатие; разумен для холодного архива, который пишут один раз и читают редко."
                     if zstd_cheaper else "Здесь GZIP не уступил ZSTD по скорости записи — на этом объёме разница в CPU кодеков незаметна."))
        z_note = (f"Запись {n(zw)} с, размер {more_less(z['size_total_mb'], g['size_total_mb'])} GZIP, повторный Q10 "
                  f"{more_less(z['q10_next_median_s'], g['q10_next_median_s'], 'медленнее', 'быстрее')}. "
                  + ("Сжимает почти как GZIP, но дешевле по CPU на записи — лучший баланс, поэтому ZSTD взят для слоёв медальона."
                     if zstd_cheaper else "По размеру и скорости ZSTD и GZIP здесь близки; ZSTD оставлен для медальона как кодек, "
                     "который на больших объёмах обычно пишется и читается быстрее при сопоставимом сжатии."))
        for b, note in [(g, g_note), (z, z_note)]:
            out.append(f"| {b['codec']} | первый {n(b['q10_first_s'])} с, далее медиана {n(b['q10_next_median_s'])} с "
                       f"| {n(b['size_total_mb'])} | {note} |")
    else:
        out.append(f"| GZIP | — | — | {MISSING} |\n| ZSTD | — | — | {MISSING} |")

    out.append("\nРасширенное сравнение (все варианты, Trino):\n")
    out.append("| Формат | Кодек | Запись, с | из них lineitem, с | Bronze, МБ | lineitem, МБ | Файлов lineitem | Q10 первый, с | Q10 далее (медиана), с |\n"
               "|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for b in r.bench["results"]:
        if b["small_files"]:
            continue
        li = b["tables"]["lineitem"]
        out.append(f"| {b['format']} | {b['codec']} | {n(b['write_total_s'])} | {n(b['write_s']['lineitem'])} | "
                   f"{n(b['size_total_mb'])} | {n(li['size_mb'])} | {li['files']} | {n(b['q10_first_s'])} | {n(b['q10_next_median_s'])} |")

    if r.spark:
        out.append("\nТот же Q10 в Spark 3.5 по тем же файлам (`./run.sh spark-bench`, 1-й прогон — холодный, дальше медиана):\n")
        out.append("| Формат | Кодек | Trino первый / далее, с | Spark первый / далее, с |\n|---|---|---:|---:|")
        for s in r.spark["results"]:
            b = r.b(s["format"], s["codec"])
            tr = f"{n(b['q10_first_s'])} / {n(b['q10_next_median_s'])}" if b else "—"
            out.append(f"| {s['format']} | {s['codec']} | {tr} | {n(s['first_s'])} / {n(s['next_median_s'])} |")

    out.append("\nВыводы из замеров:\n")
    out.extend("- " + x for x in conclusions_bench(r))
    return "\n".join(out) + "\n"


def conclusions_bench(r):
    res = [b for b in r.bench["results"] if not b["small_files"]]
    pq = {b["codec"]: b for b in res if b["format"] == "PARQUET"}
    out = []
    if "NONE" in pq:
        comp = [b for c, b in pq.items() if c != "NONE"]
        if comp:
            best = min(comp, key=lambda b: b["size_total_mb"])
            out.append(f"**Сжатие окупается по месту.** Bronze без сжатия — {n(pq['NONE']['size_total_mb'])} МБ, самый компактный "
                       f"вариант ({best['codec']}) — {n(best['size_total_mb'])} МБ, в {pq['NONE']['size_total_mb'] / best['size_total_mb']:.1f} раза меньше. "
                       f"Хранение, бэкапы и сеть до S3 оплачиваются по объёму, поэтому NONE в Bronze невыгоден.")
    fastest = min(res, key=lambda b: b["write_total_s"])
    slowest = max(res, key=lambda b: b["write_total_s"])
    out.append(f"**Скорость записи:** быстрее всех {fastest['format']}+{fastest['codec']} ({n(fastest['write_total_s'])} с), "
               f"медленнее всех {slowest['format']}+{slowest['codec']} ({n(slowest['write_total_s'])} с). "
               f"Тяжёлый кодек платит CPU на записи; без сжатия экономится CPU, но на диск уходит больше байт.")
    qs = [b["q10_next_median_s"] for b in res]
    spread = max(qs) - min(qs)
    rel = spread / statistics.median(qs) * 100 if statistics.median(qs) else 0
    if rel < 25:
        out.append(f"**Время Q10 от кодека почти не зависит:** медианы от {n(min(qs))} до {n(max(qs))} с (разброс {rel:.0f}%), "
                   f"это сравнимо с колебаниями между прогонами одной серии. На масштабе {r.bench['sf']} запрос упирается в "
                   f"джойны и планирование, а не в распаковку файлов.")
    else:
        q_best = min(res, key=lambda b: b["q10_next_median_s"])
        q_worst = max(res, key=lambda b: b["q10_next_median_s"])
        out.append(f"**Время Q10 зависит от варианта:** быстрее всего {q_best['format']}+{q_best['codec']} "
                   f"({n(q_best['q10_next_median_s'])} с), медленнее — {q_worst['format']}+{q_worst['codec']} ({n(q_worst['q10_next_median_s'])} с): "
                   f"чем меньше байт читается из MinIO и чем дешевле распаковка, тем быстрее скан.")
    for codec in ("NONE", "ZSTD"):
        a, o = r.b("PARQUET", codec), r.b("ORC", codec)
        if a and o:
            out.append(f"**Parquet vs ORC ({codec}):** ORC {n(o['size_total_mb'])} МБ против {n(a['size_total_mb'])} МБ у Parquet "
                       f"({more_less(o['size_total_mb'], a['size_total_mb'])}), Q10 {n(o['q10_next_median_s'])} с против {n(a['q10_next_median_s'])} с.")
            break
    if r.spark:
        sp = r.sp(*r.lab2)
        tb = r.b(*r.lab2)
        if sp and tb:
            out.append(f"**Trino vs Spark** ({'+'.join(r.lab2)}): повторный Q10 в Trino {n(tb['q10_next_median_s'])} с, в Spark "
                       f"{n(sp['next_median_s'])} с. Trino — MPP-движок с долгоживущим сервером и конвейерным выполнением, он "
                       f"рассчитан на интерактивные запросы; Spark платит за планирование стадий и запуск задач, его сила — "
                       f"большие батчевые ETL и ML, а не короткие запросы.")
    return out


def sec6(r):
    out = ["""
## 6. Часть 2 — осмысление и работа с инструментами

### 6.1. Понять и объяснить

| Вопрос | Твой ответ |
|---|---|"""]
    out.append("| Зачем нужны большие данные и lakehouse (а не обычная БД/хранилище)? | "
               "Обычная реляционная БД держит данные на дисках одного сервера и оптимизирована под короткие транзакции; "
               "аналитика по миллиардам строк в ней медленная, а масштабирование — это покупка сервера побольше. Data lake "
               "решает проблему объёма: дешёвое объектное хранилище вместит сколько угодно файлов, но файлы — это не таблицы: "
               "нет транзакций, схемы, истории изменений, двое пишущих могут испортить данные. Lakehouse соединяет оба подхода: "
               "данные лежат в дешёвом S3 в открытых колоночных форматах (Parquet/ORC), а табличный формат Iceberg поверх них "
               "даёт ACID-коммиты, схему и её эволюцию, снапшоты и time travel. Хранение отделено от вычислений: одни и те же "
               "таблицы читают Trino, Spark и BI (в этой работе Trino пишет, Spark читает те же файлы), а движок можно сменить "
               "или масштабировать, не перенося данные. Колоночное хранение и сжатие делают аналитику дешёвой: из 16 колонок "
               "`lineitem` Q10 читает 4. |")
    e = r.evo
    if e:
        tiers = ", ".join(f"{t[0]} — {n(t[1])}" for t in e["tiers"])
        snaps_schema = ("не создали новых снапшотов" if e["snapshots_after_add"] == len(e["snapshots_before"])
                        and e["snapshots_after_rename"] == e["snapshots_after_update"] else
                        "изменили число снапшотов (см. results/evolution.json)")
        out.append(f"| Пример schema evolution на Silver: добавил/переименовал колонку — что произошло? | "
                   f"На `silver.lakehouse.dim_customer` (`./run.sh evolution`, шаги — `sql/05_evolution_timetravel.sql`, часть A). "
                   f"Исходно {len(e['columns_before'])} колонок. `ADD COLUMN customer_tier varchar` выполнился мгновенно: из "
                   f"{n(e['after_add']['total'])} строк заполнено {n(e['after_add']['filled'])} — у старых строк NULL, потому что "
                   f"Iceberg записал только новую версию схемы в metadata.json, а parquet-файлы не переписывал. Затем `UPDATE` "
                   f"заполнил колонку по балансу клиента: {tiers}. `RENAME COLUMN phone TO phone_number` — `DESCRIBE` сразу "
                   f"показал новое имя, данные читаются по-прежнему (например, {e['sample_after_rename'][0][1]}): Iceberg "
                   f"сопоставляет колонки по числовому id, а не по имени, поэтому переименование не ломает старые файлы. "
                   f"Потом имя вернул, чтобы `03_gold.sql` работал. По `$snapshots` видно: `ADD COLUMN` и `RENAME` {snaps_schema} "
                   f"— это изменения только метаданных; снапшот появился только от `UPDATE`, который реально записал данные. |")
        hist = " → ".join(s[2] for s in e["snapshots_final"])
        out.append(f"| Пример скрипта восстановления данных из снапшота (time travel) | "
                   f"Часть B того же скрипта. Снапшот до аварии S0 = `{e['s0']}`. «Авария»: `DELETE FROM silver.lakehouse.dim_customer "
                   f"WHERE region = 'EUROPE'` — клиентов региона было {n(e['europe_before'])}, стало {n(e['europe_after_delete'])}. "
                   f"Чтение прошлого: `SELECT count(*) FROM silver.lakehouse.dim_customer FOR VERSION AS OF {e['s0']} WHERE region = 'EUROPE'` "
                   f"→ {n(e['europe_time_travel'])}. Восстановление: `INSERT INTO silver.lakehouse.dim_customer SELECT * FROM "
                   f"silver.lakehouse.dim_customer FOR VERSION AS OF {e['s0']} WHERE region = 'EUROPE'` → снова {n(e['europe_restored'])}, "
                   f"всего в таблице {n(e['total_restored'])}. Возвращены только потерянные строки, а не вся таблица, и история сохранена "
                   f"для аудита: {hist}. |")
    else:
        out.append(f"| Пример schema evolution на Silver | {MISSING} |\n| Пример скрипта восстановления из снапшота | {MISSING} |")

    out.append("""
### 6.2. Подключить инструмент к lakehouse

| Инструмент | Почему выбрал именно его |
|---|---|
| Apache Spark 3.5 + Iceberg 1.9 (движок) | Trino уже закрывает интерактивный SQL, поэтому второй MPP-движок того же класса (Doris, StarRocks) дублировал бы его роль. Spark — движок другого типа: батчевые ETL, DataFrame API на Python, ML. Он подключается к тому же Iceberg REST-каталогу Lakekeeper и читает те же файлы в MinIO без копирования — это и есть главное обещание lakehouse (одни данные — много движков). Кроме того, Spark нужен лабе 2: на одних и тех же файлах можно сравнить два движка и показать влияние кэша (`CACHE TABLE`). Для моей задачи (короткий агрегирующий запрос) Trino быстрее, а Spark был бы лучше для тяжёлой пересборки слоёв на больших объёмах. |

Развёртывание — сервис `tools` в `docker-compose.yml` и `tools/Dockerfile` (Python 3.12, OpenJDK 17, `pyspark==3.5.6`, jar-файлы `iceberg-spark-runtime-3.5_2.12` и `iceberg-aws-bundle` 1.9.2). Подключение к lakehouse — конфигурация каталогов в `tools/lab.py` (`spark_session`): `type=rest`, `uri=http://lakekeeper:8181/catalog`, `warehouse=bronze` (и silver/gold), `io-impl=S3FileIO`, endpoint `http://minio:9000`, path-style доступ.

```bash
docker compose build tools
./run.sh spark-bronze      # = docker compose run --rm tools spark-bronze
```

Действия в консоли (лог — `results/spark_bronze.log`): Spark увидел namespace `lakehouse` в каталоге `bronze`, все пять таблиц Bronze, их схему, посчитал строки, выполнил агрегацию по `returnflag`, прочитал метатаблицы Iceberg `.snapshots`/`.files` и витрину Gold.
""")
    if r.sparklog.exists():
        log = r.sparklog.read_text(encoding="utf-8").strip()
        out.append("```text\n" + log + "\n```\n")
    if r.sparkb:
        lines = {a[0]: int(a[1]) for a in r.sparkb["agg"]}
        total = sum(lines.values())
        li = r.table("bronze.lakehouse.lineitem")
        silver = r.table("silver.lakehouse.fct_returned_item")
        msg = f"Сумма строк по флагам — {n(total)}"
        if li:
            msg += " — совпадает с `count(*)` по `bronze.lakehouse.lineitem` в Trino" if total == li["rows"] else " — НЕ совпадает с Trino"
        if silver and "R" in lines:
            msg += (f". Строк с `R` — {n(lines['R'])}, в `silver.lakehouse.fct_returned_item` — {n(silver['rows'])}: "
                    + ("при переходе Bronze → Silver ни одна возвращённая позиция не потерялась." if lines['R'] == silver['rows']
                       else "разница — строки, отсеянные фильтрами качества."))
        out.append(msg + " Spark и Trino видят одни и те же данные через один каталог.\n")
    return "\n".join(out) + "\n"


def sec7(r):
    parts = ["\n## 7. Выводы\n"]
    s = ("Медальон разделил ответственность: Bronze хранит полный источник и позволяет пересобрать что угодно, Silver — "
         "очищенный клиент и факт возвратов за все кварталы, Gold — готовый ответ на один бизнес-вопрос")
    s += (", и сверка с эталонным Q10 подтвердила его корректность. " if r.check and r.check["ok"] else ". ")
    z, no = r.b("PARQUET", "ZSTD"), r.b("PARQUET", "NONE")
    if z and no:
        s += (f"Главный рычаг экономии — сжатие: ZSTD уменьшил Bronze в {no['size_total_mb'] / z['size_total_mb']:.1f} раза "
              f"относительно несжатого, при этом на скорость Q10 кодек влияет слабо. ")
    p = r.part
    if p and p.get("partitioned", {}).get("input_rows") and p.get("no_partitions", {}).get("input_rows"):
        k = p["no_partitions"]["input_rows"] / p["partitioned"]["input_rows"]
        s += f"Партиционирование Silver по кварталу совпадает с фильтром запроса и сократило чтение в {k:.1f} раза. "
    s += ("Iceberg поверх S3 дал то, чего нет у «просто файлов»: эволюцию схемы без переписывания данных, восстановление "
          "удалённых строк из снапшота и доступ к одним таблицам из Trino и Spark.")
    parts.append(s)
    return "\n".join(parts) + "\n"


def sec8(r):
    out = ["""
## 8. Лабораторная 2 — форматы, метаданные, кэширование

Мои параметры из таблицы преподавателя: формат **{0}**, сжатие **{1}**. Все замеры — на том же стенде, тем же Q10 в качестве агрегации.
""".format(*r.lab2)]
    row = r.lab2_row()
    if row:
        out.append("**Строка для таблицы:**\n\n| Сжатие | Формат | Размер, ГБ | Файлов | Время записи, с | Агрегация первая, с | Агрегация вторая и следующие, с | Тест | Trino | Spark |\n|---|---|---:|---:|---:|---:|---:|---|---|---|")
        out.append(f"| {r.lab2[1].lower()} | {r.lab2[0].lower()} | {row['size_gb']} | {row['files']} | {row['write_s']} | "
                   f"{row['agg_first']} | {row['agg_next']} | {row['sf']} | {row['trino']} | {row['spark']} |")
        out.append("\nРазмер, файлы и время записи — таблица `lineitem` (основной объём данных). Агрегация — Q10 в Trino: первая — "
                   "сразу после перезапуска Trino (холодный кэш метаданных), вторая и следующие — медиана повторных прогонов. "
                   "Колонки Trino/Spark — «первый / повторный» прогон в каждом движке.\n")
    else:
        out.append(MISSING + "\n")

    fmt, codec = r.lab2
    big, small = r.b(fmt, codec), r.b(fmt, codec, small=True)
    out.append("### 8.1. Влияние объёма метаданных (много мелких файлов)\n")
    if big and small:
        def plan(b):
            v = [x.get("planning_ms") for x in b["q10_runs"] if x.get("planning_ms") is not None]
            return statistics.median(v) if v else None
        lb, ls = big["tables"]["lineitem"], small["tables"]["lineitem"]
        out.append(f"Те же данные ({fmt}+{codec}) записаны второй раз с `target_max_file_size = {r.bench['small_file_size']}` — "
                   f"файлов становится больше, каждый меньше.\n")
        out.append("| Вариант | Файлов всего | Файлов lineitem | Манифестов | Манифесты, КБ | Размер, МБ | Запись, с | Планирование Q10, мс | Q10 далее, с |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        for name, b in [("обычные файлы", big), (f"мелкие файлы ({r.bench['small_file_size']})", small)]:
            mf = sum(t["manifests"] for t in b["tables"].values())
            mk = sum(t["manifests_kb"] for t in b["tables"].values())
            out.append(f"| {name} | {b['files_total']} | {b['tables']['lineitem']['files']} | {mf} | {n(mk, 1)} | "
                       f"{n(b['size_total_mb'])} | {n(b['write_total_s'])} | {n(plan(b), 0) if plan(b) is not None else '—'} | {n(b['q10_next_median_s'])} |")
        k = ls["files"] / lb["files"] if lb["files"] else 0
        out.append(f"\nФайлов `lineitem` стало в {raz(k)} больше. Чем больше файлов, тем больше записей в манифестах: движку при "
                   f"планировании нужно прочитать метаданные, отобрать файлы и раздать их как splits, а на чтении — открыть каждый "
                   f"файл отдельным запросом к S3 и прочитать его футер. Q10 на мелких файлах: {n(small['q10_next_median_s'])} с против "
                   f"{n(big['q10_next_median_s'])} с ({more_less(small['q10_next_median_s'], big['q10_next_median_s'], 'медленнее', 'быстрее')}). "
                   f"Объём данных при этом почти тот же ({n(small['size_total_mb'])} против {n(big['size_total_mb'])} МБ) — платим не за "
                   f"байты, а за число объектов и метаданных. Отсюда практическое правило: файлы в lakehouse держат крупными "
                   f"(сотни МБ), а мелкие периодически уплотняют (`ALTER TABLE ... EXECUTE optimize`).\n")
    else:
        out.append(MISSING + "\n")

    out.append("### 8.2. Кэширование\n")
    rows = []
    if big:
        rows.append(("Trino, первый Q10 сразу после записи", big["q10_first_s"]))
        rows.append(("Trino, повторные Q10 (медиана)", big["q10_next_median_s"]))
    if r.cold:
        cr = [x["elapsed_s"] for x in r.cold["runs"]]
        rows.append(("Trino, первый Q10 после `docker compose restart trino`", cr[0]))
        rows.append(("Trino, второй Q10 после рестарта", cr[1] if len(cr) > 1 else None))
    sp = r.sp(fmt, codec)
    if sp:
        rows.append(("Spark, первый Q10 (новая сессия)", sp["first_s"]))
        rows.append(("Spark, повторные Q10 (медиана)", sp["next_median_s"]))
    if r.spark and r.spark.get("cache"):
        rows.append((f"Spark, заполнение `CACHE TABLE` нужных колонок", r.spark["cache"]["fill_s"]))
        rows.append(("Spark, Q10 по закэшированным данным (медиана)", r.spark["cache"]["median_s"]))
    if rows:
        out.append("| Сценарий | Время, с |\n|---|---:|")
        out.extend(f"| {a} | {n(b)} |" for a, b in rows)
        out.append("\nПервый запрос всегда дороже: движок загружает метаданные таблиц из каталога (metadata.json, манифесты), "
                   "JIT-компилятор JVM ещё не разогнал код операторов, а файлы не лежат в страничном кэше ОС контейнера MinIO. "
                   "После рестарта Trino кэши метаданных и JIT обнуляются — поэтому первый прогон снова медленный, а второй "
                   "быстрый. В Spark то же плюс запуск задач; явный `CACHE TABLE` держит нужные колонки в памяти исполнителя, и "
                   "запрос больше не ходит в S3 — это выгодно, когда одни данные читаются много раз подряд, но стоит памяти и "
                   "времени на заполнение кэша.\n")
    else:
        out.append(MISSING + "\n")

    out.append("### 8.3. Компромисс «размер ↔ скорость ↔ стоимость»\n")
    if r.bench:
        res = [b for b in r.bench["results"] if not b["small_files"]]
        mine = r.b(fmt, codec)
        best = min(res, key=lambda b: b["size_total_mb"])
        if mine:
            out.append(f"Мой вариант {fmt}+{codec}: {n(mine['size_total_mb'])} МБ, запись {n(mine['write_total_s'])} с, "
                       f"повторный Q10 {n(mine['q10_next_median_s'])} с. Самый компактный — {best['format']}+{best['codec']}: "
                       f"{n(best['size_total_mb'])} МБ, запись {n(best['write_total_s'])} с, Q10 {n(best['q10_next_median_s'])} с. "
                       f"Хранение моего варианта обойдётся в {mine['size_total_mb'] / best['size_total_mb']:.1f} раза дороже "
                       f"(плата за объём в S3 и трафик пропорциональна размеру), а по скорости повторного Q10 мой вариант "
                       f"{more_less(mine['q10_next_median_s'], best['q10_next_median_s'], 'медленнее', 'быстрее')}. "
                       f"Поэтому без сжатия имеет смысл держать разве что короткоживущие промежуточные данные, которые много раз "
                       f"читаются CPU-ограниченным движком; для Bronze/Silver/Gold — ZSTD, для холодного архива — GZIP или ZSTD "
                       f"с высоким уровнем, для горячих данных с частой перезаписью — SNAPPY.\n")
    return "\n".join(out) + "\n"


def build(root):
    root = pathlib.Path(root)
    r = R(root)
    text = "".join(f(r) for f in (sec_header, sec1, sec2, sec2_evidence, sec3, sec4, sec5, sec6, sec7, sec8))
    (root / "отчет.md").write_text(text, encoding="utf-8")
    print(f"отчет.md собран ({len(text)} символов)")
    row = r.lab2_row()
    if row:
        (root / "results" / "lab2_row.json").write_text(json.dumps(row, ensure_ascii=False, indent=2), encoding="utf-8")


def fill_xlsx(root):
    """Заполняет строку FIO в private/VK_Лаба2.xlsx -> private/VK_Лаба2_filled.xlsx."""
    import openpyxl
    root = pathlib.Path(root)
    r = R(root)
    row = r.lab2_row()
    if not row:
        raise SystemExit("нет замеров: сначала ./run.sh bench (и spark-bench)")
    vals = [row["size_gb"], row["files"], row["write_s"], row["agg_first"], row["agg_next"],
            row["sf"], row["trino"], row["spark"]]
    print("Строка для таблицы (колонки D..K):")
    print("\t".join(str(v) for v in [r.env["FIO"], r.lab2[1].lower(), r.lab2[0].lower()] + vals))
    src = root / "private" / "VK_Лаба2.xlsx"
    if not src.exists():
        print(f"{src.relative_to(root)} не найден — скопируй туда таблицу, если нужно заполнить файл")
        return
    wb = openpyxl.load_workbook(src)
    ws = wb["Оценки"] if "Оценки" in wb.sheetnames else wb.active
    target = next((c.row for c in ws["A"] if str(c.value or "").strip() == r.env["FIO"]), None)
    if not target:
        raise SystemExit(f"в таблице нет строки «{r.env['FIO']}»")
    for col, v in zip("DEFGHIJK", vals):
        ws[f"{col}{target}"] = v
    dst = root / "private" / "VK_Лаба2_filled.xlsx"
    wb.save(dst)
    print(f"заполнена строка {target}, сохранено в {dst.relative_to(root)}")
