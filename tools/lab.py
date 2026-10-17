#!/usr/bin/env python3
"""Оркестратор практической №1 / лабы 2: медальон, проверки, бенчмарки Trino и Spark.

Запускается в контейнере tools (см. docker-compose.yml) командами ./run.sh <команда>.
Все замеры пишутся в results/*.json, отчёт собирается из них командой `report`.
"""
import json
import os
import pathlib
import re
import statistics
import sys
import time
import urllib.request
from datetime import datetime, timezone

ROOT = pathlib.Path(os.getenv("WORK", "/work"))
RES = ROOT / "results"
SQL_DIR = ROOT / "sql"
RES.mkdir(exist_ok=True)

SF = os.getenv("SF", "sf1").strip()
MED_CODEC = os.getenv("MEDALLION_CODEC", "ZSTD").strip().upper()
LAB2_FORMAT = os.getenv("LAB2_FORMAT", "PARQUET").strip().upper()
LAB2_CODEC = os.getenv("LAB2_CODEC", "NONE").strip().upper()
COMBOS = [tuple(c.split(":")) for c in os.getenv(
    "COMBOS", "PARQUET:NONE PARQUET:SNAPPY PARQUET:GZIP PARQUET:ZSTD ORC:NONE ORC:ZSTD").upper().split()]
RUNS = int(os.getenv("RUNS", "5"))
SMALL_FILE = os.getenv("SMALL_FILE_SIZE", "8MB").strip()
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
BENCH_TABLES = ["nation", "customer", "orders", "lineitem"]   # то, что нужно Q10
MB = 1024 * 1024


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def save(name, data):
    path = RES / name
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    log(f"записано {path.relative_to(ROOT)}")


def load(name, default=None):
    path = RES / name
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


# ----------------------------------------------------------------- Trino ----

def connect(session=None):
    import trino
    return trino.dbapi.connect(host=TRINO_HOST, port=8080, user="lab", catalog="bronze",
                               schema="lakehouse", session_properties=session or {},
                               request_timeout=300)


def q(sql, session=None):
    """Выполняет запрос, возвращает (строки, метрики). Время — серверное elapsed Trino."""
    cur = connect(session).cursor()
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    wall = time.perf_counter() - t0
    st = cur.stats or {}
    elapsed = (st.get("elapsedTimeMillis") or 0) / 1000
    return rows, {
        "query_id": cur.query_id,
        "wall_s": round(wall, 3),
        "elapsed_s": round(elapsed or wall, 3),
        "physical_input_mb": round((st.get("physicalInputBytes") or 0) / MB, 2),
        "processed_rows": st.get("processedRows"),
        "peak_memory_mb": round((st.get("peakMemoryBytes") or 0) / MB, 1),
    }


def planning_ms(query_id):
    try:
        rows, _ = q(f"SELECT planning_time_ms, analysis_time_ms FROM system.runtime.queries "
                    f"WHERE query_id = '{query_id}'")
        return {"planning_ms": rows[0][0], "analysis_ms": rows[0][1]} if rows else {}
    except Exception as e:  # колонки может не быть в другой версии Trino
        return {"planning_error": str(e)[:120]}


def statements(path):
    text = path.read_text(encoding="utf-8").replace("${SF}", SF)
    text = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in text.split(";") if s.strip()]


def run_file(name, session=None):
    log(f"--- {name}")
    out = []
    for stmt in statements(SQL_DIR / name):
        rows, m = q(stmt, session)
        log(f"    {m['elapsed_s']:>7.2f} s  {' '.join(stmt.split())[:90]}")
        out.append({"sql": " ".join(stmt.split())[:200], **m, "rows": rows[:5]})
    return out


def wait_trino(timeout=300):
    log("жду Trino…")
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            info = json.loads(urllib.request.urlopen(f"http://{TRINO_HOST}:8080/v1/info", timeout=5).read())
            if not info.get("starting", True):
                cats = {r[0] for r in q("SHOW CATALOGS")[0]}
                need = {"bronze", "silver", "gold", "tpch"}
                if need <= cats:
                    log(f"Trino готов, каталоги: {sorted(cats)}")
                    return
                log(f"нет каталогов {need - cats} — проверь trino/catalog и docker logs trino")
        except Exception:
            pass
        time.sleep(3)
    sys.exit("Trino не поднялся: docker logs trino")


def table_stats(fqn):
    """Строки, data-файлы, размер, манифесты, снапшоты и metadata.json таблицы Iceberg."""
    cat, sch, tbl = fqn.split(".")
    meta = f'{cat}.{sch}."{tbl}'
    parts = [f"(SELECT count(*) FROM {fqn})",
             f'(SELECT count(*) FROM {meta}$files" WHERE content = 0)',
             f'(SELECT coalesce(sum(file_size_in_bytes), 0) FROM {meta}$files" WHERE content = 0)',
             f'(SELECT count(*) FROM {meta}$manifests")',
             f'(SELECT coalesce(sum(length), 0) FROM {meta}$manifests")',
             f'(SELECT count(*) FROM {meta}$snapshots")']
    try:
        rows = q("SELECT " + ", ".join(parts + [f'(SELECT count(*) FROM {meta}$metadata_log_entries")']))[0][0]
    except Exception:  # на случай версии Trino без $metadata_log_entries
        rows = list(q("SELECT " + ", ".join(parts))[0][0]) + [None]
    return {"table": fqn, "rows": rows[0], "files": rows[1], "size_mb": round(rows[2] / MB, 3),
            "size_bytes": rows[2], "manifests": rows[3], "manifests_kb": round(rows[4] / 1024, 1),
            "snapshots": rows[5], "metadata_json": rows[6]}


def layer_session(codec=MED_CODEC):
    return {f"{c}.compression_codec": codec for c in ("bronze", "silver", "gold")}


# ------------------------------------------------------------- команды ----

def cmd_wait():
    wait_trino()


def cmd_medallion():
    wait_trino()
    t0 = time.time()
    steps = {}
    for f in ["00_schemas.sql", "01_bronze.sql", "02_silver.sql", "03_gold.sql"]:
        steps[f] = run_file(f, layer_session())
    tables = ["bronze.lakehouse.customer", "bronze.lakehouse.orders", "bronze.lakehouse.lineitem",
              "bronze.lakehouse.nation", "bronze.lakehouse.region",
              "silver.lakehouse.dim_customer", "silver.lakehouse.fct_returned_item",
              "gold.lakehouse.top20_returned_customers_q4_1993"]
    stats = [table_stats(t) for t in tables]
    parts, _ = q('SELECT CAST(partition.order_quarter AS varchar), record_count, file_count, total_size '
                 'FROM silver.lakehouse."fct_returned_item$partitions" ORDER BY 1')
    gold, _ = q("SELECT revenue_rank, custkey, name, round(revenue, 2), nation "
                "FROM gold.lakehouse.top20_returned_customers_q4_1993 ORDER BY revenue_rank")
    save("medallion.json", {"sf": SF, "codec": MED_CODEC, "built_in_s": round(time.time() - t0, 1),
                            "tables": stats, "silver_partitions": parts, "gold": gold,
                            "steps": steps})
    for s in stats:
        log(f"{s['table']:<52} {s['rows']:>10} строк {s['size_mb']:>9.2f} МБ {s['files']:>3} файлов")


def cmd_check():
    wait_trino()
    rows, m = q(statements(SQL_DIR / "04_check.sql")[0])
    ok = rows[0][0] == 0 and rows[0][1] == 0
    save("check.json", {"only_in_reference": rows[0][0], "only_in_gold": rows[0][1], "ok": ok})
    log("Gold совпадает с эталонным Q10" if ok else f"РАСХОЖДЕНИЕ с эталоном: {rows[0]}")


def _scan_metrics(plan_text, table):
    """Достаёт из EXPLAIN ANALYZE строку скана нужной таблицы: Input rows/size, Physical input."""
    lines = plan_text.splitlines()
    for i, line in enumerate(lines):
        if ("Scan" in line and "[table = " in line and f".{table}$" in line):
            for nxt in lines[i:i + 40]:
                if "Physical input:" in nxt:
                    m_in = re.search(r"Input: ([\d,]+) rows \(([^)]+)\)", nxt)
                    m_ph = re.search(r"Physical input: ([^,]+)", nxt)
                    m_sp = re.search(r"Splits: (\d+)", nxt)
                    return {"input_rows": int(m_in.group(1).replace(",", "")) if m_in else None,
                            "input_size": m_in.group(2) if m_in else None,
                            "physical_input": m_ph.group(1).strip() if m_ph else None,
                            "splits": int(m_sp.group(1)) if m_sp else None}
    return {}


def cmd_partition():
    """Обоснование партиционирования Silver цифрами: с партициями по кварталу и без."""
    wait_trino()
    sess = layer_session()
    q("DROP TABLE IF EXISTS silver.lakehouse.fct_returned_item_nopart")
    q("CREATE TABLE silver.lakehouse.fct_returned_item_nopart WITH (format = 'PARQUET') AS "
      "SELECT * FROM silver.lakehouse.fct_returned_item", sess)
    res = {}
    for name, tbl in [("partitioned", "fct_returned_item"), ("no_partitions", "fct_returned_item_nopart")]:
        st = table_stats(f"silver.lakehouse.{tbl}")
        sql = (f"EXPLAIN ANALYZE SELECT custkey, sum(net_revenue) FROM silver.lakehouse.{tbl} "
               f"WHERE order_quarter = DATE '1993-10-01' GROUP BY custkey")
        rows, m = q(sql)
        res[name] = {"files": st["files"], "size_mb": st["size_mb"], "elapsed_s": m["elapsed_s"],
                     **_scan_metrics(rows[0][0], tbl)}
        log(f"{name}: {res[name]}")
    q("DROP TABLE silver.lakehouse.fct_returned_item_nopart")
    save("partition.json", res)


def _snapshots(tbl):
    rows, _ = q(f'SELECT snapshot_id, CAST(committed_at AS varchar), operation '
                f'FROM silver.lakehouse."{tbl}$snapshots" ORDER BY committed_at')
    return rows


def _columns(tbl):
    return [r[0] for r in q(f"DESCRIBE silver.lakehouse.{tbl}")[0]]


def cmd_evolution():
    """Часть 2: schema evolution + восстановление из снапшота (шаги из sql/05_...)."""
    wait_trino()
    t = "silver.lakehouse.dim_customer"
    sess = layer_session()
    # приводим таблицу к исходному виду, если команду запускают повторно
    if "phone_number" in _columns("dim_customer"):
        q(f"ALTER TABLE {t} RENAME COLUMN phone_number TO phone")
    q(f"ALTER TABLE {t} DROP COLUMN IF EXISTS customer_tier")

    r = {"columns_before": _columns("dim_customer"), "snapshots_before": _snapshots("dim_customer")}
    q(f"ALTER TABLE {t} ADD COLUMN customer_tier varchar")
    r["after_add"] = dict(zip(["total", "filled"], q(f"SELECT count(*), count(customer_tier) FROM {t}")[0][0]))
    r["snapshots_after_add"] = len(_snapshots("dim_customer"))
    q(f"UPDATE {t} SET customer_tier = CASE WHEN acctbal >= 5000 THEN 'HIGH' "
      f"WHEN acctbal >= 0 THEN 'REGULAR' ELSE 'DEBTOR' END", sess)
    r["tiers"] = q(f"SELECT customer_tier, count(*) FROM {t} GROUP BY 1 ORDER BY 1")[0]
    r["snapshots_after_update"] = len(_snapshots("dim_customer"))
    q(f"ALTER TABLE {t} RENAME COLUMN phone TO phone_number")
    r["columns_after_rename"] = _columns("dim_customer")
    r["sample_after_rename"] = q(f"SELECT custkey, phone_number FROM {t} ORDER BY custkey LIMIT 3")[0]
    r["snapshots_after_rename"] = len(_snapshots("dim_customer"))
    q(f"ALTER TABLE {t} RENAME COLUMN phone_number TO phone")

    s0 = _snapshots("dim_customer")[-1][0]
    r["s0"] = s0
    r["europe_before"] = q(f"SELECT count(*) FROM {t} WHERE region = 'EUROPE'")[0][0][0]
    q(f"DELETE FROM {t} WHERE region = 'EUROPE'", sess)
    r["europe_after_delete"] = q(f"SELECT count(*) FROM {t} WHERE region = 'EUROPE'")[0][0][0]
    r["europe_time_travel"] = q(f"SELECT count(*) FROM {t} FOR VERSION AS OF {s0} "
                                f"WHERE region = 'EUROPE'")[0][0][0]
    q(f"INSERT INTO {t} SELECT * FROM {t} FOR VERSION AS OF {s0} WHERE region = 'EUROPE'", sess)
    r["europe_restored"] = q(f"SELECT count(*) FROM {t} WHERE region = 'EUROPE'")[0][0][0]
    r["total_restored"] = q(f"SELECT count(*) FROM {t}")[0][0][0]
    r["snapshots_final"] = _snapshots("dim_customer")
    save("evolution.json", r)
    log(f"EUROPE: {r['europe_before']} -> {r['europe_after_delete']} -> "
        f"(time travel {r['europe_time_travel']}) -> {r['europe_restored']}")


def q10_sql(cat, schema, tag):
    return f"""SELECT /* {tag} */ c.custkey, c.name, sum(l.extendedprice * (1 - l.discount)) AS revenue,
       c.acctbal, n.name AS nation, c.address, c.phone, c.comment
FROM {cat}.{schema}.customer AS c
JOIN {cat}.{schema}.orders   AS o ON c.custkey = o.custkey
JOIN {cat}.{schema}.lineitem AS l ON l.orderkey = o.orderkey
JOIN {cat}.{schema}.nation   AS n ON c.nationkey = n.nationkey
WHERE o.orderdate >= DATE '1993-10-01' AND o.orderdate < DATE '1994-01-01'
  AND l.returnflag = 'R'
GROUP BY c.custkey, c.name, c.acctbal, n.name, c.address, c.phone, c.comment
ORDER BY revenue DESC
LIMIT 20"""


def bench_schema(fmt, codec, small=False):
    return f"bench_{fmt}_{codec}".lower() + ("_small" if small else "")


def bench_one(fmt, codec, small=False):
    schema = bench_schema(fmt, codec, small)
    sess = {"bronze.compression_codec": codec}
    if small:
        sess["bronze.target_max_file_size"] = SMALL_FILE
    log(f">>> {fmt} + {codec}{' (мелкие файлы ' + SMALL_FILE + ')' if small else ''} -> bronze.{schema}")
    q(f"CREATE SCHEMA IF NOT EXISTS bronze.{schema}")
    writes, tables = {}, {}
    for t in BENCH_TABLES:
        q(f"DROP TABLE IF EXISTS bronze.{schema}.{t}")
        _, m = q(f"CREATE TABLE bronze.{schema}.{t} WITH (format = '{fmt}') AS "
                 f"SELECT * FROM tpch.{SF}.{t}", sess)
        writes[t] = m["elapsed_s"]
        tables[t] = table_stats(f"bronze.{schema}.{t}")
    runs = []
    for i in range(1, RUNS + 1):
        _, m = q(q10_sql("bronze", schema, f"bench {schema} run{i}"))
        runs.append({**m, **planning_ms(m["query_id"])})
    times = [r["elapsed_s"] for r in runs]
    res = {
        "format": fmt, "codec": codec, "small_files": small, "schema": schema, "sf": SF,
        "write_s": writes, "write_total_s": round(sum(writes.values()), 2),
        "tables": tables,
        "size_total_mb": round(sum(x["size_mb"] for x in tables.values()), 2),
        "files_total": sum(x["files"] for x in tables.values()),
        "q10_runs": runs, "q10_first_s": times[0],
        "q10_next_median_s": round(statistics.median(times[1:]), 3) if len(times) > 1 else times[0],
        "q10_median_s": round(statistics.median(times), 3),
    }
    li = tables["lineitem"]
    log(f"    запись {res['write_total_s']} s (lineitem {writes['lineitem']} s), "
        f"размер {res['size_total_mb']} МБ (lineitem {li['size_mb']} МБ, {li['files']} файлов), "
        f"Q10 первый {res['q10_first_s']} s, далее медиана {res['q10_next_median_s']} s")
    return res


def cmd_bench():
    wait_trino()
    combos = list(COMBOS)
    if (LAB2_FORMAT, LAB2_CODEC) not in combos:
        combos.insert(0, (LAB2_FORMAT, LAB2_CODEC))
    results = [bench_one(f, c) for f, c in combos]
    # опыт с метаданными: тот же формат/кодек лабы 2, но много мелких файлов
    results.append(bench_one(LAB2_FORMAT, LAB2_CODEC, small=True))
    save("bench_trino.json", {"sf": SF, "runs": RUNS, "lab2": [LAB2_FORMAT, LAB2_CODEC],
                              "small_file_size": SMALL_FILE, "results": results})


def cmd_trino_cold():
    """Запускается сразу после `docker compose restart trino` (это делает run.sh):
    первый Q10 — с холодными кэшами метаданных и JIT, второй — уже прогретый."""
    wait_trino()
    schema = bench_schema(LAB2_FORMAT, LAB2_CODEC)
    out = []
    for i in range(1, 4):
        _, m = q(q10_sql("bronze", schema, f"cold {schema} run{i}"))
        out.append({**m, **planning_ms(m["query_id"])})
        log(f"    после рестарта, прогон {i}: {m['elapsed_s']} s")
    save("trino_cold.json", {"schema": schema, "runs": out})


# ----------------------------------------------------------------- Spark ----

def spark_session(app):
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.master("local[*]").appName(app)
         .config("spark.jars", "/opt/jars/iceberg-spark-runtime.jar,/opt/jars/iceberg-aws-bundle.jar")
         .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.shuffle.partitions", "16")
         .config("spark.ui.enabled", "false"))
    for cat in ("bronze", "silver", "gold"):
        p = f"spark.sql.catalog.{cat}"
        b = (b.config(p, "org.apache.iceberg.spark.SparkCatalog")
             .config(f"{p}.type", "rest")
             .config(f"{p}.uri", "http://lakekeeper:8181/catalog")
             .config(f"{p}.warehouse", cat)
             .config(f"{p}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
             .config(f"{p}.s3.endpoint", "http://minio:9000")
             .config(f"{p}.s3.path-style-access", "true")
             .config(f"{p}.s3.access-key-id", "minio")
             .config(f"{p}.s3.secret-access-key", "minio12345")
             .config(f"{p}.client.region", "us-east-1"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _timed(spark, sql):
    t0 = time.perf_counter()
    rows = spark.sql(sql).collect()
    return round(time.perf_counter() - t0, 3), rows


def cmd_spark_bench():
    """Q10 в Spark по тем же таблицам Bronze, что писал Trino: первый прогон (холодный
    JVM, метаданные Iceberg ещё не загружены), повторные, и прогон по закэшированным данным."""
    bench = load("bench_trino.json")
    if not bench:
        sys.exit("сначала ./run.sh bench")
    t0 = time.perf_counter()
    spark = spark_session("q10-bench")
    startup = round(time.perf_counter() - t0, 2)
    order = [(LAB2_FORMAT, LAB2_CODEC)] + [(r["format"], r["codec"]) for r in bench["results"]
                                            if not r["small_files"] and
                                            (r["format"], r["codec"]) != (LAB2_FORMAT, LAB2_CODEC)]
    results = []
    for fmt, codec in order:
        schema = bench_schema(fmt, codec)
        times = []
        for i in range(RUNS):
            sec, rows = _timed(spark, q10_sql("bronze", schema, f"spark {i}"))
            times.append(sec)
        results.append({"format": fmt, "codec": codec, "schema": schema, "runs": times,
                        "first_s": times[0],
                        "next_median_s": round(statistics.median(times[1:]), 3),
                        "top1": [str(x) for x in rows[0][:3]]})
        log(f"Spark {fmt}+{codec}: {times}")

    # кэширование: нужные колонки lab2-таблиц в памяти Spark, тот же Q10 по ним
    schema = bench_schema(LAB2_FORMAT, LAB2_CODEC)
    cols = {"customer": "custkey, name, acctbal, nationkey, address, phone, comment",
            "orders": "orderkey, custkey, orderdate",
            "lineitem": "orderkey, extendedprice, discount, returnflag",
            "nation": "nationkey, name"}
    t0 = time.perf_counter()
    for t, c in cols.items():
        spark.sql(f"CACHE TABLE c_{t} AS SELECT {c} FROM bronze.{schema}.{t}")
    cache_fill = round(time.perf_counter() - t0, 2)
    cached_q = (q10_sql("bronze", schema, "cached")
                .replace(f"bronze.{schema}.customer", "c_customer")
                .replace(f"bronze.{schema}.orders", "c_orders")
                .replace(f"bronze.{schema}.lineitem", "c_lineitem")
                .replace(f"bronze.{schema}.nation", "c_nation"))
    cached = [_timed(spark, cached_q)[0] for _ in range(RUNS)]
    log(f"Spark cache: заполнение {cache_fill} s, Q10 по кэшу {cached}")
    spark.stop()
    save("bench_spark.json", {"startup_s": startup, "results": results,
                              "cache": {"schema": schema, "fill_s": cache_fill, "runs": cached,
                                        "median_s": round(statistics.median(cached), 3)}})


def cmd_spark_bronze():
    """П. 6.2: Spark подключается к тому же REST-каталогу и читает Bronze (лог — evidence)."""
    lines = []

    def show(title, df_or_rows):
        rows = df_or_rows.collect() if hasattr(df_or_rows, "collect") else df_or_rows
        block = [f"> {title}"] + ["  " + " | ".join(str(v) for v in r) for r in rows]
        lines.extend(block + [""])
        print("\n".join(block), flush=True)

    spark = spark_session("bronze-demo")
    show("SHOW NAMESPACES IN bronze", spark.sql("SHOW NAMESPACES IN bronze"))
    show("SHOW TABLES IN bronze.lakehouse", spark.sql("SHOW TABLES IN bronze.lakehouse"))
    show("DESCRIBE bronze.lakehouse.lineitem (первые 6 колонок)",
         spark.sql("DESCRIBE bronze.lakehouse.lineitem").limit(6))
    counts = [(t, spark.table(f"bronze.lakehouse.{t}").count())
              for t in ["customer", "orders", "lineitem", "nation", "region"]]
    show("count(*) по таблицам Bronze", counts)
    agg_sql = ("SELECT returnflag, count(*) AS lines, round(sum(extendedprice * (1 - discount)), 2) "
               "AS revenue FROM bronze.lakehouse.lineitem GROUP BY returnflag ORDER BY returnflag")
    sec, agg = _timed(spark, agg_sql)
    show(f"{agg_sql}   -- {sec} s", agg)
    show("Снапшоты bronze.lakehouse.lineitem (метатаблица Iceberg .snapshots)",
         spark.sql("SELECT snapshot_id, committed_at, operation FROM bronze.lakehouse.lineitem.snapshots"))
    show("Файлы bronze.lakehouse.lineitem (метатаблица .files)",
         spark.sql("SELECT count(*) AS files, round(sum(file_size_in_bytes)/1048576, 2) AS mb, "
                   "first(file_format) AS format FROM bronze.lakehouse.lineitem.files"))
    show("Gold, прочитанный Spark (сверка с Trino)",
         spark.sql("SELECT revenue_rank, custkey, round(revenue, 2) FROM "
                   "gold.lakehouse.top20_returned_customers_q4_1993 ORDER BY revenue_rank LIMIT 5"))
    spark.stop()
    (RES / "spark_bronze.log").write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    save("spark_bronze.json", {"counts": counts, "agg": [list(map(str, r)) for r in agg],
                               "agg_s": sec})


# ---------------------------------------------------------------------------

def cmd_report():
    sys.path.insert(0, str(ROOT / "tools"))
    import report
    report.build(ROOT)


def cmd_xlsx():
    sys.path.insert(0, str(ROOT / "tools"))
    import report
    report.fill_xlsx(ROOT)


COMMANDS = {
    "wait": cmd_wait, "medallion": cmd_medallion, "check": cmd_check, "partition": cmd_partition,
    "evolution": cmd_evolution, "bench": cmd_bench, "trino-cold": cmd_trino_cold,
    "spark-bench": cmd_spark_bench, "spark-bronze": cmd_spark_bronze,
    "report": cmd_report, "xlsx": cmd_xlsx,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        sys.exit("команды: " + ", ".join(COMMANDS))
    log(f"{sys.argv[1]}  (SF={SF}, medallion={MED_CODEC}, lab2={LAB2_FORMAT}+{LAB2_CODEC})")
    COMMANDS[sys.argv[1]]()
