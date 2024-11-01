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
