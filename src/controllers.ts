import { BrokerAsPromised, SubscriberSessionAsPromised } from "rascal";
import logger from "./logger";

const INACTIVE_CONTROLLER_TIMEOUT = 1000 * 60; // 1 minute
const INACTIVE_CONTROLLER_CHECK_INTERVAL = 1000 * 30; // 30 seconds
const MINIMUM_BATCHES_FOR_REPORTING = 2;
const CONTROLLER_CACHE: Cache<Controller> = {};
let batchCount = 0;
let lastBatchId: string | null = null;

export async function addListener(
  subscription: SubscriberSessionAsPromised,
  broker: BrokerAsPromised
) {
  subscription.on("message", async (_: any, content: any, ackOrNack: any) => {
    await processRawController(content?.data, content?.batchId, broker);
    await ackOrNack();
  });

  // Check for inactive controllers every 10 seconds.
  setInterval(async () => {
    logger.debug("Checking for inactive controllers");
    await checkForInactiveControllers(broker);
  }, INACTIVE_CONTROLLER_CHECK_INTERVAL);
}

export async function processRawController(
  controller: Controller,
  batchId: string,
  broker: BrokerAsPromised
) {
  if (batchId && batchId !== lastBatchId) {
    lastBatchId = batchId;
    batchCount++;
    logger.debug(
      `New batch: ${batchId}. Batch count: ${batchCount}. Cache Ready: ${
        batchCount > MINIMUM_BATCHES_FOR_REPORTING
      }`
    );
  }

  const key = `${controller.cid}-${controller.callsign}`;
  const cacheEntry = CONTROLLER_CACHE[key];

  if (cacheEntry) {
    cacheEntry.timestamp = Date.now();
  } else {
    logger.debug(`Controller ${key} is not in cache. Adding to cache.`);
    CONTROLLER_CACHE[key] = { timestamp: Date.now(), value: controller };
    // Only publish connect events after minimum batches
    if (batchCount > MINIMUM_BATCHES_FOR_REPORTING) {
      logger.debug(`Controller ${key} is connected. Publishing connect event.`);
      await broker.publish("events.controller.connect", {
        event: "connect",
        data: controller,
        timestamp: Date.now(),
      });
    }
  }

  if (batchId && batchId !== lastBatchId) {
    lastBatchId = batchId;
    batchCount++;
  }
}

export async function checkForInactiveControllers(broker: BrokerAsPromised) {
  if (batchCount < MINIMUM_BATCHES_FOR_REPORTING) return;
  const currentTime = Date.now();
  for (const key in CONTROLLER_CACHE) {
    const cacheEntry = CONTROLLER_CACHE[key];
    if (currentTime - cacheEntry.timestamp > INACTIVE_CONTROLLER_TIMEOUT) {
      logger.debug(`Controller ${key} is inactive. Disconnecting.`);
      logger.silly(
        `Time since last seen: ${currentTime - cacheEntry.timestamp}`
      );
      delete CONTROLLER_CACHE[key];
      await broker.publish("events.controller.disconnect", {
        event: "disconnect",
        data: cacheEntry.value,
        timestamp: currentTime,
      });
    }
  }
}

type Cache<T> = Record<string, { timestamp: number; value: T }>;

export type Controller = {
  cid: number;
  name: string;
  callsign: string;
  frequency: string;
  facility: number;
  rating: number;
  server: string;
  visual_range: number;
  text_atis: string[];
  last_updated: string;
  logon_time: string;
};
