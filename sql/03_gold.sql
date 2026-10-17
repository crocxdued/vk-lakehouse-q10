-- GOLD: витрина — топ-20 клиентов по выручке от возвратов за IV квартал 1993 (ответ на Q10).
-- revenue_rank добавлен для BI: место клиента в рейтинге без сортировки на стороне отчёта.

DROP TABLE IF EXISTS gold.lakehouse.top20_returned_customers_q4_1993;

CREATE TABLE gold.lakehouse.top20_returned_customers_q4_1993 WITH (format = 'PARQUET') AS
SELECT
  row_number() OVER (ORDER BY revenue DESC) AS revenue_rank,
  custkey, name, revenue, acctbal, nation, address, phone, comment
FROM (
  SELECT
    c.custkey,
    c.name,
    sum(f.net_revenue) AS revenue,
    c.acctbal,
    c.nation,
    c.address,
    c.phone,
    c.comment
  FROM silver.lakehouse.fct_returned_item AS f
  JOIN silver.lakehouse.dim_customer      AS c ON f.custkey = c.custkey
  WHERE f.order_quarter = DATE '1993-10-01'
  GROUP BY c.custkey, c.name, c.acctbal, c.nation, c.address, c.phone, c.comment
  ORDER BY revenue DESC
  LIMIT 20
) AS top;
