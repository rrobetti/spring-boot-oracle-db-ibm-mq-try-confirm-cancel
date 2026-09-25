# Testing

## Run all tests

```bash
mvn clean verify
```

## Run a single integration test

```bash
mvn verify -Dit.test=OrderServiceIT#happyPath_dbCommitsAndMessagesBecomeVisible
```

## Debugging failing tests

- Use `mvn -X verify -Dit.test=OrderServiceIT` for verbose logs.
- Use Testcontainers logs from Maven output to inspect Oracle and MQ startup.
- The test client connects directly to MQ, while application connections pass through Toxiproxy.
- Toxiproxy toxics are cleaned in `@AfterEach`; verify no toxic is left active.

## Adding a new test

- Keep tests integration-focused and run through Spring Boot context.
- Prefer Awaitility for async assertions.
- Keep queue setup in `99-test-queues.mqsc` aligned with tested destinations.
