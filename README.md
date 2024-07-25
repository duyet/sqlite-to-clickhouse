# SQLite to ClickHouse

## Overview

This script facilitates the transfer of data from an SQLite database to a ClickHouse database. It automates the process of schema inference, data insertion, and optimization, ensuring efficient data migration.

## Installation

Follow these steps to set up the environment:

```bash
virtualenv ./venv
source ./venv/bin/activate
pip3 install -r requirements.txt
```

## Usage

```bash
python3 ./main.py --help

usage: main.py [-h] --sqlite SQLITE --clickhouse-host CLICKHOUSE_HOST --clickhouse-port CLICKHOUSE_PORT --clickhouse-user CLICKHOUSE_USER --clickhouse-password CLICKHOUSE_PASSWORD
               --clickhouse-database CLICKHOUSE_DATABASE [--max-workers MAX_WORKERS]

Transfer data from SQLite to ClickHouse.

options:
  -h, --help            show this help message and exit
  --sqlite SQLITE       Path to the SQLite database.
  --clickhouse-host CLICKHOUSE_HOST
                        ClickHouse host.
  --clickhouse-port CLICKHOUSE_PORT
                        ClickHouse port.
  --clickhouse-user CLICKHOUSE_USER
                        ClickHouse user.
  --clickhouse-password CLICKHOUSE_PASSWORD
                        ClickHouse password.
  --clickhouse-database CLICKHOUSE_DATABASE
                        ClickHouse database name.
  --max-workers MAX_WORKERS
                        Number of worker threads (default: 4).
```

Example:

```bash
python3 ./main.py --sqlite ~/Downloads/home-assistant_v2.db \
    --clickhouse-host duet-ubuntu \
    --clickhouse-user duyet \
    --clickhouse-password 123 \
    --clickhouse-database home-assistant

2024-07-25 15:04:42,056 - INFO - Connected to SQLite database: /Users/duet/Downloads/home-assistant_v2.db
2024-07-25 15:04:42,059 - INFO - Connected to ClickHouse at duet-ubuntu:9000
2024-07-25 15:04:42,059 - INFO - Processing table: event_data
2024-07-25 15:04:42,109 - INFO - ClickHouse schema event_data: data_id -> Int64, hash -> String, shared_data -> String
2024-07-25 15:04:42,263 - INFO - Inserted 1537 rows into event_data
2024-07-25 15:04:42,263 - INFO - Processing table: event_types
2024-07-25 15:04:42,295 - INFO - ClickHouse schema event_types: event_type_id -> Int64, event_type -> String
2024-07-25 15:04:42,321 - INFO - Inserted 30 rows into event_types
2024-07-25 15:04:42,322 - INFO - Processing table: state_attributes
2024-07-25 15:04:42,366 - INFO - ClickHouse schema state_attributes: attributes_id -> Int64, hash -> String, shared_attrs -> String
2024-07-25 15:04:42,993 - INFO - Inserted 2576 rows into state_attributes
2024-07-25 15:04:42,993 - INFO - Processing table: states_meta
2024-07-25 15:04:43,077 - INFO - ClickHouse schema states_meta: metadata_id -> Int64, entity_id -> String
2024-07-25 15:04:43,136 - INFO - Inserted 351 rows into states_meta
2024-07-25 15:04:43,136 - INFO - Processing table: statistics_meta
2024-07-25 15:04:43,178 - INFO - ClickHouse schema statistics_meta: id -> Int64, statistic_id -> String, source -> String, unit_of_measurement -> String, has_mean -> String, has_sum -> String, name -> String
2024-07-25 15:04:43,201 - INFO - Inserted 45 rows into statistics_meta
2024-07-25 15:04:43,201 - INFO - Processing table: recorder_runs
2024-07-25 15:04:43,231 - INFO - ClickHouse schema recorder_runs: run_id -> Int64, start -> DateTime, end -> DateTime, closed_incorrect -> String, created -> DateTime
2024-07-25 15:04:43,274 - INFO - Inserted 26 rows into recorder_runs
2024-07-25 15:04:43,274 - INFO - Processing table: migration_changes
2024-07-25 15:04:43,303 - INFO - ClickHouse schema migration_changes: migration_id -> String, version -> String
2024-07-25 15:04:43,330 - INFO - Inserted 4 rows into migration_changes
2024-07-25 15:04:43,330 - INFO - Processing table: schema_changes
2024-07-25 15:04:43,366 - INFO - ClickHouse schema schema_changes: change_id -> Int64, schema_version -> Int64, changed -> DateTime
2024-07-25 15:04:43,413 - INFO - Inserted 1 rows into schema_changes
2024-07-25 15:04:43,413 - INFO - Processing table: statistics_runs
2024-07-25 15:04:43,464 - INFO - ClickHouse schema statistics_runs: run_id -> Int64, start -> DateTime
2024-07-25 15:04:43,538 - INFO - Inserted 2986 rows into statistics_runs

```

## License

MIT
