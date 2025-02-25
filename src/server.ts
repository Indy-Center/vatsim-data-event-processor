import logger from "./logger";
import { BrokerAsPromised } from "rascal";
import rabbitConfig from "./rabbitConfig";
import { addFlightPlanListener } from "./flight-plans";

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

  await addFlightPlanListener(
    [
      await broker.subscribe("raw.flight_plans"),
      await broker.subscribe("raw.prefiles"),
    ],
    broker
  );
}

main().catch((error) => {
  logger.error(`Fatal error: ${error}`);
  process.exit(1);
});
