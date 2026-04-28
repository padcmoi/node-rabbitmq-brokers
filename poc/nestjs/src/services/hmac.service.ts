import { randomUUID } from "node:crypto";
import { Injectable, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { initializeHmacMessageAuth, type InitializedHmacMessageAuth } from "@naskot/node-hmac-auth";
import type { SignedHmacMessage } from "@naskot/node-rabbitmq-brokers";
import { createClient, type RedisClientType } from "redis";
import { appConfig } from "../config.js";

@Injectable()
export class HmacService implements OnModuleInit, OnModuleDestroy {
  private redisClient: RedisClientType | null = null;
  private messageAuth: InitializedHmacMessageAuth | null = null;

  async onModuleInit() {
    this.redisClient = createClient({ url: appConfig.redisUrl });
    await this.redisClient.connect();

    this.messageAuth = initializeHmacMessageAuth({
      redis: this.redisClient,
      namespace: appConfig.hmac.namespace,
      secretToken: appConfig.hmac.secretToken,
    });

    for (const credential of appConfig.hmac.credentials) {
      const existing = await this.messageAuth.clients.get(credential.clientId);
      if (existing) continue;

      await this.messageAuth.clients.create({
        clientId: credential.clientId,
        plainSecret: credential.secret,
      });

      console.info("[nestjs] hmac credential created", { clientId: credential.clientId });
    }
  }

  async onModuleDestroy() {
    if (this.redisClient?.isOpen) {
      await this.redisClient.quit();
    }
  }

  private getInstance() {
    if (!this.messageAuth) {
      throw new Error("hmac service not initialized");
    }

    return this.messageAuth;
  }

  async signMessage(clientId: string, payload: unknown) {
    const signed = await this.getInstance().signMessage({
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

  async verifyMessage(message: SignedHmacMessage) {
    try {
      await this.getInstance().verifyMessage({
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
}
