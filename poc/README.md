# POC Docker: Express <-> NestJS Secure Ping-Pong

This POC starts 4 containers:

- `rabbitmq`
- `redis`
- `express-api`
- `nestjs-api`

No HTTP ports are exposed for Express/NestJS.
The goal is secure bidirectional RabbitMQ messages with logs in both app consoles.

## Start everything

```bash
cd poc
docker compose up --build
```

## What you should see

In both `express-api` and `nestjs-api` logs:

- startup of RabbitMQ consumers
- secure incoming messages verified by HMAC
- ping/pong exchange in both directions:
  - express -> nest (ping), nest -> express (pong)
  - nest -> express (ping), express -> nest (pong)

## Useful commands

```bash
# Follow all logs
docker compose logs -f

# Follow one API only
docker compose logs -f express-api
docker compose logs -f nestjs-api

# Stop

docker compose down -v
```
