-- Часть 2 (п. 6.1): schema evolution и восстановление из снапшота.
-- Автоматически выполняется командой ./run.sh evolution (tools/lab.py подставляет
-- snapshot_id сам и пишет результат в results/evolution.json).
-- Для ручного прогона: docker exec -it trino trino, шаги по порядку, <S0> — snapshot_id из B1.

-- A1. Схема до изменений
DESCRIBE silver.lakehouse.dim_customer;
-- A2. Новая колонка: файлы не переписываются, у старых строк значение NULL
ALTER TABLE silver.lakehouse.dim_customer ADD COLUMN customer_tier varchar;
SELECT count(*) AS total, count(customer_tier) AS filled FROM silver.lakehouse.dim_customer;
-- A3. Заполняем колонку (это уже запись данных => новый снапшот)
UPDATE silver.lakehouse.dim_customer
SET customer_tier = CASE WHEN acctbal >= 5000 THEN 'HIGH' WHEN acctbal >= 0 THEN 'REGULAR' ELSE 'DEBTOR' END;
SELECT customer_tier, count(*) FROM silver.lakehouse.dim_customer GROUP BY 1 ORDER BY 1;
-- A4. Переименование: Iceberg хранит колонки по id, данные не трогаются
ALTER TABLE silver.lakehouse.dim_customer RENAME COLUMN phone TO phone_number;
SELECT custkey, phone_number FROM silver.lakehouse.dim_customer LIMIT 3;
-- A5. Имя возвращаем, чтобы 03_gold.sql продолжал работать
ALTER TABLE silver.lakehouse.dim_customer RENAME COLUMN phone_number TO phone;

-- B1. Текущий снапшот (S0) — точка, к которой будем восстанавливаться
SELECT snapshot_id, committed_at, operation FROM silver.lakehouse."dim_customer$snapshots" ORDER BY committed_at;
-- B2. «Авария»: удалили всех клиентов региона EUROPE
SELECT count(*) FROM silver.lakehouse.dim_customer WHERE region = 'EUROPE';
DELETE FROM silver.lakehouse.dim_customer WHERE region = 'EUROPE';
-- B3. Time travel: в прошлой версии строки на месте
SELECT count(*) FROM silver.lakehouse.dim_customer FOR VERSION AS OF <S0> WHERE region = 'EUROPE';
-- B4. Скрипт восстановления: возвращаем только потерянные строки из снапшота S0
INSERT INTO silver.lakehouse.dim_customer
SELECT * FROM silver.lakehouse.dim_customer FOR VERSION AS OF <S0> WHERE region = 'EUROPE';
-- B5. Проверка и история: append -> overwrite -> delete -> append
SELECT count(*) FROM silver.lakehouse.dim_customer WHERE region = 'EUROPE';
SELECT snapshot_id, committed_at, operation FROM silver.lakehouse."dim_customer$snapshots" ORDER BY committed_at;
