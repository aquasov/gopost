create table if not exists events (
    client_date Date,
    client_time DateTime,
    device_id String,
    device_os LowCardinality(String),
    session String,
    sequence Int8,
    event LowCardinality(String),
    attributes Nested(
        key LowCardinality(String),
        value String
    ),
    metrics Nested(
        key LowCardinality(String),
        value Float64
    )
) engine = MergeTree()
partition by (device_os, toYYYYMM(client_time))
order by (client_date, event);


create table if not exists events_buffer as events
engine = Buffer(default, events, 8, 1, 10, 1000, 10000, 256000, 2560000);