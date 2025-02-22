import logger from "./logger";
import { BrokerAsPromised } from "rascal";
import rabbitConfig from "./rabbitConfig";
import { addListener } from "./controllers";

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

  await addListener(await broker.subscribe("raw.controllers"), broker);
}

main().catch((error) => {
  logger.error(`Fatal error: ${error}`);
  process.exit(1);
});
