import { randomUUID } from "node:crypto";
import { RabbitMQClient, RabbitMqBrokerRouter } from "@naskot/node-rabbitmq-brokers";
import { appConfig } from "../config.js";
import { createExpressToNestAction, type PingPongPayload } from "../actions/expressToNest.action.js";
import { createNestToExpressAction } from "../actions/nestToExpress.action.js";
import { signHmacMessage, verifyHmacMessage } from "./hmac.service.js";

const MESSAGE_MARKER = "***MESSAGE_FROM_NASKOT***";

const rabbitClient = new RabbitMQClient("POC_EXPRESS", appConfig.rabbitmq, {
  hmac: {
    signMessage: signHmacMessage,
    verifyMessage: verifyHmacMessage,
  },
  logger: {
    info: (meta, msg) => console.info(msg, meta),
    warn: (meta, msg) => console.warn(msg, meta),
    error: (meta, msg) => console.error(msg, meta),
    debug: (meta, msg) => console.info(msg, meta),
  },
});

const brokerRouter = new RabbitMqBrokerRouter({
  client: rabbitClient,
  queue: appConfig.broker.queue,
  bindingKey: appConfig.broker.bindingKey,
  topicExchange: appConfig.broker.topicExchange,
  fanoutExchange: appConfig.broker.fanoutExchange,
  unknownTypeRetryDelayMs: appConfig.broker.unknownTypeRetryDelayMs,
  logger: {
    info: (meta, msg) => console.info(msg, meta),
    warn: (meta, msg) => console.warn(msg, meta),
    error: (meta, msg) => console.error(msg, meta),
    debug: (meta, msg) => console.info(msg, meta),
  },
});

const expressToNestAction = createExpressToNestAction();
const nestToExpressAction = createNestToExpressAction(async (incoming) => {
  await publishExpressToNest({
    kind: "pong",
    text: `express pong for ${incoming.threadId}`,
    threadId: incoming.threadId,
  });
});

let initialPingSent = false;

brokerRouter.registerAction(nestToExpressAction);

function scheduleStartupPings() {
  const startupDelaysMs = [1500, 5000];

  for (const delayMs of startupDelaysMs) {
    setTimeout(() => {
      void publishExpressToNest({
        kind: "ping",
        text: "express startup ping",
        threadId: randomUUID(),
      });
    }, delayMs);
  }
}

export async function bootstrapRabbitMqService() {
  await brokerRouter.startConsumer({
    allowedKeyIds: ["nest_app"],
    prefetchCount: 5,
  });

  if (initialPingSent) return;
  initialPingSent = true;

  scheduleStartupPings();
}

export async function publishExpressToNest(input: { kind: "ping" | "pong"; text: string; threadId: string }) {
  const taggedText = input.text.includes(MESSAGE_MARKER) ? input.text : `${MESSAGE_MARKER} ${input.text}`;

  console.info("[express] sending to nest", {
    marker: MESSAGE_MARKER,
    kind: input.kind,
    threadId: input.threadId,
    text: taggedText,
  });

  const payload: PingPongPayload = {
    from: "express",
    kind: input.kind,
    text: taggedText,
    threadId: input.threadId,
    at: Date.now(),
  };

  return await brokerRouter.publish(
    "express_app",
    expressToNestAction,
    {
      message: `${MESSAGE_MARKER} [express] ${input.kind}`,
      payload,
    },
    [appConfig.broker.publishRoutingKeyToNest]
  );
}
