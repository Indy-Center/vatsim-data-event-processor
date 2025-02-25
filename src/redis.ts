import { createClient } from "redis";
import logger from "./logger";

const FLIGHT_PLAN_TTL = 1000 * 60 * 10; // 10 minutes

class RedisService {
  private client;
  private subClient;

  constructor() {
    this.client = createClient({
      url: process.env.REDIS_URL || "redis://localhost:6379",
    });

    // Separate client for subscription (Redis requirement)
    this.subClient = this.client.duplicate();

    this.client.on("error", (err: any) =>
      logger.error("Redis Client Error", err)
    );
    this.subClient.on("error", (err: any) =>
      logger.error("Redis Sub Client Error", err)
    );
  }

  async connect() {
    await this.client.connect();
    await this.subClient.connect();

    // Enable keyspace notifications for expired events
    await this.client.configSet("notify-keyspace-events", "Ex");
  }

  async setFlightPlan(key: string, data: any) {
    // Store flight plan (without TTL)
    await this.client.set(key, JSON.stringify(data));

    // Set TTL key that references the flight plan key
    await this.client.set(`ttl:${key}`, key, {
      EX: Math.floor(FLIGHT_PLAN_TTL / 1000), // Redis expects seconds
    });
    logger.debug(`Set flight plan and TTL for key: ${key}`);
  }

  async getFlightPlan(key: string) {
    const data = await this.client.get(key);
    return data ? JSON.parse(data) : null;
  }

  async refreshTTL(key: string) {
    // Refresh TTL on the TTL key
    const ttlKey = `ttl:${key}`;
    const success = await this.client.expire(
      ttlKey,
      Math.floor(FLIGHT_PLAN_TTL / 1000)
    );
    if (!success) {
      logger.debug(`Failed to refresh TTL for key: ${key} - TTL key not found`);
      // If TTL key doesn't exist, recreate it
      await this.client.set(ttlKey, key, {
        EX: Math.floor(FLIGHT_PLAN_TTL / 1000),
      });
      logger.debug(`Recreated TTL key for: ${key}`);
    } else {
      logger.debug(`Refreshed TTL for key: ${key}`);
    }
  }

  async subscribeToExpiration(callback: (key: string) => Promise<void>) {
    await this.subClient.subscribe(
      "__keyevent@0__:expired",
      async (message: string) => {
        logger.debug(`Received expiration for key: ${message}`);

        // Only handle our TTL keys
        if (!message.startsWith("ttl:")) {
          return;
        }

        // Get the actual flight plan key from the TTL key
        const flightPlanKey = message.substring(4); // Remove 'ttl:' prefix

        // Get the flight plan data (it should still exist)
        const data = await this.getFlightPlan(flightPlanKey);
        if (data) {
          logger.debug(
            `Processing expiration for flight plan: ${flightPlanKey}`
          );
          // Process expiration
          await callback(flightPlanKey);
          // Clean up the flight plan data
          await this.client.del(flightPlanKey);
          logger.debug(`Cleaned up expired flight plan: ${flightPlanKey}`);
        } else {
          logger.debug(
            `No data found for expired flight plan: ${flightPlanKey}`
          );
        }
      }
    );
  }

  async getRemainingTTL(key: string): Promise<number> {
    return await this.client.ttl(key);
  }

  async findKeysByPattern(pattern: string): Promise<string[]> {
    return await this.client.keys(pattern);
  }

  async deleteFlightPlan(key: string) {
    await this.client.del(key);
    await this.client.del(`ttl:${key}`);
    logger.debug(`Deleted flight plan and TTL for key: ${key}`);
  }
}

export const redisService = new RedisService();
