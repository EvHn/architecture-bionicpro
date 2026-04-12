CREATE TABLE IF NOT EXISTS emg_sensor_data (
    user_id UInt32,
    prosthesis_type String,
    muscle_group String,
    signal_frequency UInt32,
    signal_duration UInt32,
    signal_amplitude Decimal(5,2),
    signal_time DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, prosthesis_type, signal_time);

INSERT INTO emg_sensor_data
SELECT *
FROM file('olap.csv', 'CSV');

CREATE TABLE IF NOT EXISTS customers_queue
(
    before String,
    after String, 
    source String,
    op String,
    ts_ms UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'dbserver1.public.customers',
    kafka_group_name = 'clickhouse_customers_consumer',
    kafka_format = 'JSONEachRow'; 

CREATE TABLE IF NOT EXISTS customers
(
    id Int32,
    name String,
    email String,
    op String, 
    ts_ms UInt64
)
ENGINE = MergeTree()
ORDER BY (id, ts_ms);

CREATE MATERIALIZED VIEW IF NOT EXISTS customers_mv
TO customers
AS
SELECT
    JSONExtractInt(after, 'id') AS id,
    JSONExtractString(after, 'name') AS name,
    JSONExtractString(after, 'email') AS email,
    op,
    ts_ms
FROM customers_queue
WHERE op IN ('c', 'r'); 