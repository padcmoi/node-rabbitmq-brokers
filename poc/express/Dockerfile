FROM node:20-alpine

RUN corepack enable
ENV CI=true

WORKDIR /app

COPY . .

RUN pnpm install --frozen-lockfile && pnpm build
RUN pnpm --dir poc/express install --frozen-lockfile

WORKDIR /app/poc/express

ENV RABBITMQ_PROTOCOL=amqp
ENV RABBITMQ_HOST=rabbitmq
ENV RABBITMQ_PORT=5672
ENV RABBITMQ_USER=guest
ENV RABBITMQ_PASSWORD=guest
ENV RABBITMQ_VHOST=/
ENV REDIS_URL=redis://redis:6379
ENV HMAC_NAMESPACE=poc:rabbitmq:messages
ENV HMAC_SECRET_TOKEN=poc_hmac_secret_token

CMD ["pnpm", "dev"]
