-- Сверка Gold с эталонным Q10 прямо по генератору tpch.${SF}.
-- Ожидаем 0 и 0: ни одного клиента/выручки, которые есть только в одной из сторон.
WITH ref AS (
  SELECT c.custkey, sum(l.extendedprice * (1 - l.discount)) AS revenue
  FROM tpch.${SF}.customer AS c
  JOIN tpch.${SF}.orders   AS o ON c.custkey = o.custkey
  JOIN tpch.${SF}.lineitem AS l ON l.orderkey = o.orderkey
  JOIN tpch.${SF}.nation   AS n ON c.nationkey = n.nationkey
  WHERE o.orderdate >= DATE '1993-10-01' AND o.orderdate < DATE '1994-01-01'
    AND l.returnflag = 'R'
  GROUP BY c.custkey
  ORDER BY revenue DESC
  LIMIT 20
),
gold AS (SELECT custkey, revenue FROM gold.lakehouse.top20_returned_customers_q4_1993)
SELECT
  (SELECT count(*) FROM (SELECT custkey, round(revenue, 2) FROM ref
                         EXCEPT SELECT custkey, round(revenue, 2) FROM gold) AS a) AS only_in_reference,
  (SELECT count(*) FROM (SELECT custkey, round(revenue, 2) FROM gold
                         EXCEPT SELECT custkey, round(revenue, 2) FROM ref) AS b) AS only_in_gold;
