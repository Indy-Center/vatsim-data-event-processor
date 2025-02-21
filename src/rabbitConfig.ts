import rascal, { BindingConfig } from "rascal";
import { config } from "./config";

const VATSIM_TYPES = ["pilots", "controllers", "atis", "prefiles"];

const queues = {
  "controller-processor-queue": {
    options: {
      durable: true,
    },
  },
};

const bindings = ["vatsim.raw[raw.controllers] -> controller-processor-queue"];

const subscriptions = {
  "raw.controllers": {
    queue: "controller-processor-queue",
  },
};

const publications = {
  "events.controller.connect": {
    exchange: "vatsim.events",
    routingKey: "events.controller.connect",
  },
  "events.controller.disconnect": {
    exchange: "vatsim.events",
    routingKey: "events.controller.disconnect",
  },
  "events.controller.update": {
    exchange: "vatsim.events",
    routingKey: "events.controller.update",
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
