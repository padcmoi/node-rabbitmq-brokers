import type { BrokerActionPublishInput, RabbitMqBrokerActionContract } from "@naskot/node-rabbitmq-brokers";

export type PingPongPayload = {
  from: "express" | "nest";
  kind: "ping" | "pong";
  text: string;
  threadId: string;
  at: number;
};

export function createNestToExpressAction() {
  const action: RabbitMqBrokerActionContract<PingPongPayload> = {
    type: "nest/hello",
    publish: (input: BrokerActionPublishInput<PingPongPayload>) => {
      return {
        message: input.message,
        data: JSON.stringify(input.payload),
        timestamp: Date.now(),
      };
    },
    consume: () => {
      // producer-only action on nest side
    },
  };

  return action;
}
