-- SILVER: очищенные, нормализованные и обогащённые данные — только то, что нужно запросу.

DROP TABLE IF EXISTS silver.lakehouse.dim_customer;
DROP TABLE IF EXISTS silver.lakehouse.fct_returned_item;

-- 1) Измерение «клиент»: customer ⋈ nation ⋈ region.
--    Очистка: trim строк, верхний регистр сегмента, отсев пустых ключей,
--    дедупликация по custkey (если Bronze дозагрузят повторно — берём последнюю загрузку).
--    Обогащение: название страны и региона вместо nationkey.
CREATE TABLE silver.lakehouse.dim_customer WITH (format = 'PARQUET') AS
SELECT custkey, name, address, phone, acctbal, mktsegment, comment, nation, region
FROM (
  SELECT
    c.custkey,
    trim(c.name)             AS name,
    trim(c.address)          AS address,
    trim(c.phone)            AS phone,
    c.acctbal,
    upper(trim(c.mktsegment)) AS mktsegment,
    trim(c.comment)          AS comment,
    trim(n.name)             AS nation,
    trim(r.name)             AS region,
    row_number() OVER (PARTITION BY c.custkey ORDER BY c._ingested_at DESC) AS rn
  FROM bronze.lakehouse.customer AS c
  JOIN bronze.lakehouse.nation   AS n ON c.nationkey = n.nationkey
  JOIN bronze.lakehouse.region   AS r ON n.regionkey = r.regionkey
  WHERE c.custkey IS NOT NULL
) AS t
WHERE rn = 1;

-- 2) Факт «возвращённая позиция»: lineitem ⋈ orders, только returnflag = 'R'.
--    Выручка строки считается один раз здесь, а не в каждом запросе.
--    order_quarter — первый день квартала заказа; по нему партиционирование:
--    запрос Q10 фильтрует ровно один квартал, Trino читает одну партицию.
--    Хранятся все кварталы (не только IV-1993) — слой пригоден для любого периода.
CREATE TABLE silver.lakehouse.fct_returned_item
WITH (format = 'PARQUET', partitioning = ARRAY['order_quarter']) AS
SELECT
  l.orderkey,
  l.linenumber,
  o.custkey,
  o.orderdate,
  CAST(date_trunc('quarter', o.orderdate) AS date) AS order_quarter,
  l.extendedprice,
  l.discount,
  l.extendedprice * (1 - l.discount)               AS net_revenue
FROM bronze.lakehouse.lineitem AS l
JOIN bronze.lakehouse.orders   AS o ON l.orderkey = o.orderkey
WHERE l.returnflag = 'R'
  AND l.extendedprice > 0
  AND l.discount BETWEEN 0 AND 1;
