# cdc-lite

A lightweight Change Data Capture (CDC) solution for DuckDB, designed for simplicity and ease of use.

## Problem:

- Debezium requires significant infrastructure (Kafka, Connect workers, Zookeeper)
- High operational overhead for small/medium teams
- Complex configuration and maintenance
- Overkill for simpler use cases (small datasets, lower change volumes)
- Resource-intensive for cloud deployments

## Lite CDC Value Proposition:

- Zero External Dependencies
- Runs directly on source/target databases
- No message queue infrastructure needed
- Lower operational costs and complexity

## Project Structure

```
cdc-lite/
├── cmd/
│   └── cdc-lite/        # Main application
│       └── main.go
├── pkg/
│   └── duckdb/          # DuckDB CDC implementation
│       ├── duckdb_cdc.go
│       └── duckdb_cdc_test.go
├── scripts/             # Database setup scripts
│   └── setup.sql
└── cdc_output/          # CDC output directory
```

## Quick Setup

1. Build the project:
```bash
go build -o cdc-lite ./cmd/cdc-lite
```

2. Create and populate the source database:
```bash
# Create the source database and run setup script
duckdb source.duckdb < scripts/setup.sql

# Verify the data
duckdb source.duckdb "SELECT * FROM users;"
```

3. Run the CDC monitor:
```bash
./cdc-lite
```

4. In another terminal window, make some changes to demonstrate CDC capturing:
```bash
# Insert a new user
duckdb source.duckdb "INSERT INTO users (id, name, email) VALUES (3, 'Bob Wilson', 'bob@example.com');"

# Wait a few seconds, then update an existing user
duckdb source.duckdb "UPDATE users SET email = 'john.doe@example.com' WHERE id = 1;"

# Wait a few seconds, then delete a user
duckdb source.duckdb "DELETE FROM users WHERE id = 2;"
```

5. View the captured changes:
```bash
# List the CDC output files
ls -l cdc_output/

# View the contents of the change files
cat cdc_output/changes_*.jsonl
```

## Target Users:

- Small/medium development teams
- Startups with limited DevOps resources
- Projects requiring quick POCs
- Edge computing scenarios
- Development/testing environments

Think of it as "SQLite vs PostgreSQL" - while Debezium is enterprise-grade and feature-rich, there's a clear need for a lightweight alternative that prioritizes simplicity and ease of use over advanced features.

## Development

To run tests:
```bash
go test ./pkg/duckdb/...
```

## License

This project is licensed under the terms of the LICENSE file included in the repository.
