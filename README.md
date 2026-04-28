# @naskot/node-rabbitmq-brokers

`@naskot/node-rabbitmq-brokers` is a framework-agnostic TypeScript library for building RabbitMQ broker layers with optional HMAC message signing.

## Goal

The library provides a shared foundation to:

- manage AMQP connection and topology with resilience
- publish and consume signed messages
- route business actions by `type`
- stay portable across frameworks (Express, NestJS, others)

## Configuration Rule

The library does not read `process.env`.

Runtime variables should be resolved in your application service layer, then passed as plain objects (RabbitMQ config, HMAC services, router options).

## Available APIs and Helpers

### `RabbitMQPool`

Responsibilities:

- share connection and channel instances by namespace
- reconnect automatically on connection loss
- expose RabbitMQ primitives (exchange, queue, bind, consume, ack/nack)
- support cleanup of empty retry queues

### `RabbitMQClient`

Responsibilities:

- publish messages through `publish(...)`
- consume messages through `consume(...)`
- plug in an injectable HMAC strategy with `signMessage` and `verifyMessage`
- enforce consumer-side security checks (allowed key ids, reject/requeue)

### `RabbitMqBrokerRouter`

Responsibilities:

- register actions through `registerAction(...)`
- publish actions through `publish(...)` in fanout or topic mode
- start consumption through `startConsumer(...)`
- route incoming messages to the proper action by `type`

### `brokerRouterHelpers`

Exposed helpers:

- `normalizeActionType(...)`: normalizes and validates action `type`
- `parsePayloadData(...)`: defensively parses `payload.data`

### Exported Types

The package also exports all types required for strict TypeScript integration (config, payloads, action contracts, HMAC services, etc.).

## Integration Guides

- [Express Guide](./docs/express.md)
- [NestJS Guide](./docs/nestjs.md)

## POC

A complete POC is available here:

- [POC Express <-> NestJS](./poc/README.md)

## Quality

Standard scripts:

- `npm run lint`
- `npm run typecheck`
- `npm test`
- `npm run build`
