# CHANGELOG

## [0.1.0] - 2026-04-28

- Added framework-agnostic RabbitMQ core:
  - `RabbitMQPool` with shared connections and resilient channel lifecycle.
  - `RabbitMQClient` with signed publish/consume hooks (`signMessage` / `verifyMessage`).
  - `RabbitMqBrokerRouter` with action routing, topology bootstrap, and unknown-type defer handling.
- Added full TypeScript contracts and utility modules:
  - `src/types.ts`, `src/guards.ts`, `src/logger.ts`, `src/pool.ts`, `src/client.ts`, `src/broker-router.ts`.
- Added tests for broker router helpers and action handling:
  - `test/broker-router.test.ts`.
- Added documentation:
  - Updated main `README.md`.
  - Added integration guides in `docs/express.md` and `docs/nestjs.md`.
- Added Dockerized POC in `poc/`:
  - Express and NestJS apps using `@naskot/node-rabbitmq-brokers` + `@naskot/node-hmac-auth`.
  - Secure bidirectional ping/pong over RabbitMQ message signatures.
  - Worker mode for both APIs (no exposed HTTP ports).
  - Orchestration with RabbitMQ + Redis via `poc/docker-compose.yml`.

## [0.0.0] - 2026-04-23

- First commit
