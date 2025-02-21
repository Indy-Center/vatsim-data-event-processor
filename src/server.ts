import logger from "./logger";
import { BrokerAsPromised } from "rascal";
import rabbitConfig from "./rabbitConfig";

interface ControllerState {
  data: any;
  lastSeen: number;
}

const controllerCache: Record<string, ControllerState> = {};
const GRACE_PERIOD = 1000 * 60 * 2; // 2 minutes grace period for reconnects

process.on("SIGINT", () => {
  logger.info("SIGINT received, shutting down...");
  process.exit(0);
});

process.on("SIGTERM", () => {
  logger.info("SIGTERM received, shutting down...");
  process.exit(0);
});

async function main() {
  logger.info("Starting VATSIM Data Event Processor");

  // Create Rascal broker
  const broker = await BrokerAsPromised.create(rabbitConfig);

  broker.on("error", (err) => {
    logger.error(`Broker error: ${err}`);
  });

  const controllerSubscription = broker.subscribe("raw.controllers");

  (await controllerSubscription).on(
    "message",
    async (message: any, content: any, ackOrNack: any) => {
      await processRawController(broker, content);
      await ackOrNack();
    }
  );

  // Report controllers as disconnected if their last seen is older than 1 minute.
  setInterval(async () => {
    logger.debug("Checking for disconnected controllers");
    const currentTime = Date.now();
    const disconnectedControllers = Object.entries(controllerCache).filter(
      ([key, state]) => {
        return currentTime - state.lastSeen > GRACE_PERIOD;
      }
    );

    logger.debug(
      `Found ${disconnectedControllers.length} disconnected controllers`
    );

    for (const [key, state] of disconnectedControllers) {
      logger.debug(`Reporting controller ${key} as disconnected`);
      await broker.publish("events.controller.disconnect", {
        event: "disconnect",
        timestamp: currentTime,
        data: state.data,
      });
      delete controllerCache[key];
    }
  }, 1000 * 60);
}

async function processRawController(broker: BrokerAsPromised, message: any) {
  const controller = message.data;
  const currentTime = Date.now();

  const controllerKey = `${controller.cid}-${controller.callsign}`;

  if (controllerCache[controllerKey]) {
    logger.debug(
      `Controller ${controllerKey} is already in cache. Updating last seen.`
    );
    controllerCache[controllerKey].lastSeen = currentTime;
  } else {
    logger.debug(
      `Controller ${controllerKey} is not in cache. Adding to cache.`
    );
    controllerCache[controllerKey] = {
      data: controller,
      lastSeen: currentTime,
    };

    await broker.publish("events.controller.connect", {
      event: "connect",
      data: controller,
      timestamp: currentTime,
    });
  }
}

main().catch((error) => {
  logger.error(`Fatal error: ${error}`);
  process.exit(1);
});
