import type { BrokerActionPublishInput, RabbitMqBrokerActionContract } from "@naskot/node-rabbitmq-brokers";

export type PingPongPayload = {
  from: "express" | "nest";
  kind: "ping" | "pong";
  text: string;
  threadId: string;
  at: number;
};

export function createExpressToNestAction() {
  const action: RabbitMqBrokerActionContract<PingPongPayload> = {
    type: "express/hello",
    publish: (input: BrokerActionPublishInput<PingPongPayload>) => {
      return {
        message: input.message,
        data: JSON.stringify(input.payload),
        timestamp: Date.now(),
      };
    },
    consume: () => {
      // producer-only action on express side
    },
  };

  return action;
}
