-- BRONZE: сырые таблицы TPC-H «как пришли» — все колонки источника без изменений
-- плюс техническая колонка _ingested_at (когда загружено). Без партиций.
-- ${SF} подставляется из .env, кодек — сессией: bronze.compression_codec.
-- region добавлен сверх подсказки: нужен Silver для обогащения клиента регионом.

DROP TABLE IF EXISTS bronze.lakehouse.customer;
DROP TABLE IF EXISTS bronze.lakehouse.orders;
DROP TABLE IF EXISTS bronze.lakehouse.lineitem;
DROP TABLE IF EXISTS bronze.lakehouse.nation;
DROP TABLE IF EXISTS bronze.lakehouse.region;

CREATE TABLE bronze.lakehouse.customer WITH (format = 'PARQUET') AS
SELECT *, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at FROM tpch.${SF}.customer;

CREATE TABLE bronze.lakehouse.orders WITH (format = 'PARQUET') AS
SELECT *, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at FROM tpch.${SF}.orders;

CREATE TABLE bronze.lakehouse.lineitem WITH (format = 'PARQUET') AS
SELECT *, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at FROM tpch.${SF}.lineitem;

CREATE TABLE bronze.lakehouse.nation WITH (format = 'PARQUET') AS
SELECT *, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at FROM tpch.${SF}.nation;

CREATE TABLE bronze.lakehouse.region WITH (format = 'PARQUET') AS
SELECT *, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at FROM tpch.${SF}.region;
