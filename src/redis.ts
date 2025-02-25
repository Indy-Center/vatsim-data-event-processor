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
    const fullKey = `flight-plan:${key}`;
    await this.client.set(fullKey, JSON.stringify(data), {
      EX: Math.floor(FLIGHT_PLAN_TTL / 1000), // Redis expects seconds
    });
  }

  async refreshTTL(key: string) {
    const fullKey = `flight-plan:${key}`;
    await this.client.expire(fullKey, Math.floor(FLIGHT_PLAN_TTL / 1000));
  }

  async getFlightPlan(key: string) {
    const data = await this.client.get(`flight-plan:${key}`);
    return data ? JSON.parse(data) : null;
  }

  async subscribeToExpiration(callback: (key: string) => void) {
    // Subscribe to keyspace events for expiration
    await this.subClient.subscribe("__keyevent@0__:expired", (message) => {
      if (message.startsWith("flight-plan:")) {
        callback(message.replace("flight-plan:", ""));
      }
    });
  }
}

export const redisService = new RedisService();
