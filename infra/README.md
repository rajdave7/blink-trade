# infra — Blink-Trade local infra

## What this brings up
- Zookeeper (2181)
- Kafka broker (9092 exposed on localhost; internal: kafka:29092)
- Schema Registry (8081)
- Prometheus (9090)
- Grafana (3000; default admin/admin)
- Postgres (5432)

## Quickstart
1. From repo root:
   cd infra
   docker-compose up -d

2. Verify services:
   docker-compose ps
   # view logs
   docker-compose logs -f kafka

3. Create Kafka topics (run from host):
   docker-compose exec kafka bash -c \
     "kafka-topics --bootstrap-server kafka:29092 --create --topic raw.market --partitions 6 --replication-factor 1"
   docker-compose exec kafka bash -c \
     "kafka-topics --bootstrap-server kafka:29092 --create --topic normalized.market --partitions 6 --replication-factor 1"
   docker-compose exec kafka bash -c \
     "kafka-topics --bootstrap-server kafka:29092 --create --topic orderbook.snapshots --partitions 6 --replication-factor 1"
   docker-compose exec kafka bash -c \
     "kafka-topics --bootstrap-server kafka:29092 --create --topic metrics --partitions 1 --replication-factor 1"

4. Access UI:
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:3000 (admin/admin)
   - Schema Registry: http://localhost:8081

