# spring-boot-oracle-db-ibm-mq-try-confirm-cancel
Staged Syncpoint Publish — a Spring Boot reference implementation that stages MQ messages (invisible) before DB commit, then makes them visible only after the DB transaction succeeds. Uses local transactions only (no XA/Atomikos), with chaos-driven integration tests via Testcontainers + Toxiproxy.
