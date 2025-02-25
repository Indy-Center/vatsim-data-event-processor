import rascal, { BindingConfig } from "rascal";
import { config } from "./config";

const queues = {
  "flight-plan-processor-queue": {
    options: {
      durable: true,
    },
  },
};

const bindings = [
  "vatsim.raw[raw.flight_plans] -> flight-plan-processor-queue",
  "vatsim.raw[raw.prefiles] -> flight-plan-processor-queue",
];

const subscriptions = {
  "raw.flight_plans": {
    queue: "flight-plan-processor-queue",
  },
  "raw.prefiles": {
    queue: "flight-plan-processor-queue",
  },
};

const publications = {
  "events.flight_plan.file": {
    exchange: "vatsim.events",
    routingKey: "events.flight_plan.file",
  },
  "events.flight_plan.expire": {
    exchange: "vatsim.events",
    routingKey: "events.flight_plan.expire",
  },
  "events.flight_plan.update": {
    exchange: "vatsim.events",
    routingKey: "events.flight_plan.update",
  },
};

export default rascal.withDefaultConfig({
  vhosts: {
    "/": {
      connection: {
        url: config.RABBIT_URL,
      },
      exchanges: {
        "vatsim.raw": {
          type: "topic",
          options: { durable: true },
        },
        "vatsim.events": {
          type: "topic",
          options: { durable: true },
        },
      },
      queues,
      publications,
      subscriptions,
      bindings,
    },
  },
});
