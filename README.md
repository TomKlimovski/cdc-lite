# cdc-lite

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


## Simple Deployment


- Single binary deployment
- Configuration via YAML/JSON
- Quick setup (minutes vs hours/days)
- Ideal for dev/test environments

## Resource Efficient

- Minimal memory footprint
- Reduced cloud infrastructure costs
- Suitable for edge computing/IoT scenarios

## Target Users:

- Small/medium development teams
- Startups with limited DevOps resources
- Projects requiring quick POCs
- Edge computing scenarios
- Development/testing environments

Think of it as "SQLite vs PostgreSQL" - while Debezium is enterprise-grade and feature-rich, there's a clear need for a lightweight alternative that prioritizes simplicity and ease of use over advanced features.

# Quick setup

First, create and populate the source database:
### Create the source database and run setup script
duckdb source.duckdb < setup.sql

### Verify the data
duckdb source.duckdb "SELECT * FROM users;"

In another terminal window, make some changes to demonstrate CDC capturing:
### Insert a new user
duckdb source.duckdb "INSERT INTO users (id, name, email) VALUES (3, 'Bob Wilson', 'bob@example.com');"

### Wait a few seconds, then update an existing user
duckdb source.duckdb "UPDATE users SET email = 'john.doe@example.com' WHERE id = 1;"

### Wait a few seconds, then delete a user
duckdb source.duckdb "DELETE FROM users WHERE id = 2;"

### List the CDC output files
ls -l cdc_output/

### View the contents of the change files
cat cdc_output/changes_*.jsonl