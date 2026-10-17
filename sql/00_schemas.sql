-- Схема lakehouse в каждом слое (каталог = слой = warehouse = бакет MinIO).
-- Имена таблиц всегда трёхчастные: каталог.схема.таблица.
CREATE SCHEMA IF NOT EXISTS bronze.lakehouse;
CREATE SCHEMA IF NOT EXISTS silver.lakehouse;
CREATE SCHEMA IF NOT EXISTS gold.lakehouse;
