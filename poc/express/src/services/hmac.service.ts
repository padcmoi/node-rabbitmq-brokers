import { randomUUID } from "node:crypto";
import { initializeHmacMessageAuth, type InitializedHmacMessageAuth } from "@naskot/node-hmac-auth";
import { createClient, type RedisClientType } from "redis";
import type { SignedHmacMessage } from "@naskot/node-rabbitmq-brokers";
import { appConfig } from "../config.js";

let redisClient: RedisClientType | null = null;
let hmacMessageAuth: InitializedHmacMessageAuth | null = null;
let bootstrapPromise: Promise<void> | null = null;

async function ensureCredentials() {
  if (!hmacMessageAuth) {
    throw new Error("hmac message auth is not initialized");
  }

  for (const credential of appConfig.hmac.credentials) {
    const existing = await hmacMessageAuth.clients.get(credential.clientId);
    if (existing) continue;

    await hmacMessageAuth.clients.create({
      clientId: credential.clientId,
      plainSecret: credential.secret,
    });

    console.info("[express] hmac credential created", { clientId: credential.clientId });
  }
}

export async function bootstrapHmacService() {
  if (bootstrapPromise) {
    await bootstrapPromise;
    return;
  }

  bootstrapPromise = (async () => {
    redisClient = createClient({ url: appConfig.redisUrl });
    await redisClient.connect();

    hmacMessageAuth = initializeHmacMessageAuth({
      redis: redisClient,
      namespace: appConfig.hmac.namespace,
      secretToken: appConfig.hmac.secretToken,
    });

    await ensureCredentials();
  })();

  await bootstrapPromise;
}

function getMessageAuth() {
  if (!hmacMessageAuth) {
    throw new Error("hmac service not bootstrapped");
  }

  return hmacMessageAuth;
}

export async function signHmacMessage(clientId: string, payload: unknown) {
  await bootstrapHmacService();

  const signed = await getMessageAuth().signMessage({
    clientId,
    message: payload,
  });

  return {
    meta: {
      keyId: signed.clientId,
      tsMs: Date.now(),
      nonce: randomUUID(),
      sig: signed.signature,
    },
    payload,
  } satisfies SignedHmacMessage;
}

export async function verifyHmacMessage(message: SignedHmacMessage) {
  await bootstrapHmacService();

  try {
    await getMessageAuth().verifyMessage({
      clientId: message.meta.keyId,
      message: message.payload,
      signature: message.meta.sig,
    });

    return {
      ok: true,
    };
  } catch (error) {
    return {
      ok: false,
      code: "hmac_verify_failed",
      message: error instanceof Error ? error.message : "hmac verification failed",
      requeueOnFail: true,
      requeueDelayMs: 3000,
    };
  }
}
