[![Build status](https://github.com/rrobetti/spring-boot-oracle-db-ibm-mq-try-confirm-cancel/actions/workflows/ci.yml/badge.svg)](https://github.com/rrobetti/spring-boot-oracle-db-ibm-mq-try-confirm-cancel/actions?query=workflow%3Aci)

# Spring Boot 3 + IBM MQ + Oracle — Staged Syncpoint Publish with Local Transactions

Spring Boot sample implementing a 3-local-transaction pattern (listener JMS, publisher JMS, and JDBC) without XA/JTA.

## Architecture

```mermaid
flowchart LR
    A[ORDERS.IN listener session] --> B[Manual publisher JMS session]\
    B --> C[Oracle @Transactional DB work]\
    C --> D[Publisher session commit]\
    D --> E[Listener session commit]
```

| Transaction | Scope | Commit order |
| --- | --- | --- |
| Listener session (outer) | Consume `ORDERS.IN` | 3 |
| Publisher session (middle) | Publish to `NOTIFY.QUEUE.1` and `NOTIFY.QUEUE.2` under syncpoint | 2 |
| JDBC transaction (inner) | Persist order row | 1 |

### Why not XA?

This project intentionally avoids XA/JTA. It uses local JMS and local JDBC transactions coordinated manually in code, accepting at-least-once semantics and a crash window between DB commit and JMS commit.

## Prerequisites

- Java 21
- Maven 3.9+
- Docker (or Docker socket access) for Testcontainers
- No local Oracle or IBM MQ installation required for tests

## Quick Start

```bash
mvn clean verify
mvn spring-boot:run
```

## Configuration

| Property | Description | Default |
| --- | --- | --- |
| `ibm.mq.host` | MQ host | `localhost` |
| `ibm.mq.port` | MQ port | `1414` |
| `ibm.mq.queue-manager` | MQ queue manager | `QM1` |
| `ibm.mq.channel` | MQ channel | `SYSTEM.DEF.SVRCONN` |
| `ibm.mq.user` | MQ user | `app` |
| `ibm.mq.password` | MQ password | `passw0rd` |
| `spring.datasource.url` | Oracle JDBC URL | `jdbc:oracle:thin:@localhost:1521/FREEPDB1` |
| `spring.datasource.username` | Oracle user | `orders` |
| `spring.datasource.password` | Oracle password | `orders` |
| `spring.jpa.hibernate.ddl-auto` | Hibernate schema mode | `update` |

## Testing

All tests are integration tests.

| Test | What it proves |
| --- | --- |
| `happyPath_dbCommitsAndMessagesBecomeVisible` | DB commit then message visibility and ORDERS.IN consumption |
| `dbFails_messagesRolledBack` | DB failure rolls back outgoing publishes and incoming consume |
| `commitOrder_dbBeforeJms` | DB commit timestamp is before outgoing message visibility |
| `mqDownDuringPublish_dbRollsBack` | MQ failure during publish prevents DB commit and triggers redelivery |
| `mqDownDuringCommit_dbCommits_messageLost_redelivery` | Crash window: DB committed, outgoing invisible, incoming redelivered |
| `oracleDownDuringDbWork_messagesRolledBack` | Oracle failure rolls back publisher session and incoming message |
| `idempotency_duplicateMessage` | Duplicate order ID remains single-row and avoids duplicate notifications |

Tests require Docker and typically take ~3-5 minutes due to container startup.

Run one test:

```bash
mvn verify -Dit.test=OrderServiceIT#happyPath_dbCommitsAndMessagesBecomeVisible
```

## Design Decisions / Trade-offs

- There is an unavoidable crash window between DB commit and publisher JMS commit.
- Publisher session is manually managed to defer commit until after DB commit.
- Delivery semantics are at-least-once; idempotency is enforced via unique `order_id`.

| Approach | Pros | Cons |
| --- | --- | --- |
| Local transactions (this project) | No XA coordinator, simpler runtime | Crash window, app-managed coordination |
| XA/JTA | Atomic multi-resource commit | Operational complexity and XA overhead |
| Transactional outbox | Strong recovery model | Extra table/process and eventual publish delay |

See `docs/ARCHITECTURE.md` and `docs/TESTING.md` for details.

## References

- https://github.com/rrobetti/spring-boot-atomikos-oracle-db-ibm-mq
- https://www.ibm.com/docs/en/ibm-mq/latest?topic=applications-syncpoint-considerations-in-mq-programs
- https://docs.spring.io/spring-framework/reference/integration/jms/using.html
