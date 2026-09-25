# Architecture

## Sequence of local transactions

```mermaid
sequenceDiagram
    participant L as Listener Session (ORDERS.IN)
    participant P as Publisher Session (MQ syncpoint)
    participant DB as Oracle Local TX

    L->>P: create transacted session
    P->>P: MQPUT NOTIFY.QUEUE.1 (pending)
    P->>P: MQPUT NOTIFY.QUEUE.2 (pending)
    P->>DB: @Transactional save order
    DB-->>P: commit
    P->>P: MQCMIT (messages visible)
    P-->>L: return
    L->>L: commit ORDERS.IN consume
```

## Failure modes

- DB failure: publisher session rolls back, outgoing messages are discarded, listener rolls back incoming message.
- MQ publish failure: DB work is not committed, listener rolls back incoming message.
- Crash window (after DB commit, before publisher commit): DB is committed, outgoing messages are not visible, incoming message is redelivered.

This flow provides at-least-once processing with explicit idempotency on `order_id`.
