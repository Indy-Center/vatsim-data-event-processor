import { BrokerAsPromised, SubscriberSessionAsPromised } from "rascal";
import logger from "./logger";
import { pick } from "lodash";

const INACTIVE_FLIGHT_PLAN_TIMEOUT = 1000 * 60; // 1 minute
const INACTIVE_FLIGHT_PLAN_CHECK_INTERVAL = 1000 * 30; // 30 seconds
const FLIGHT_PLAN_CACHE: Cache<FlightPlan> = {};

export async function addFlightPlanListener(
  subscriptions: SubscriberSessionAsPromised[],
  broker: BrokerAsPromised
) {
  for (const subscription of subscriptions) {
    subscription.on("message", async (_: any, content: any, ackOrNack: any) => {
      await processRawFlightPlan(content?.data, broker);
      await ackOrNack();
    });
  }

  // Check for inactive controllers every 10 seconds.
  setInterval(async () => {
    logger.debug("Checking for inactive flight plans");
  }, INACTIVE_FLIGHT_PLAN_CHECK_INTERVAL);
}

export async function processRawFlightPlan(
  pilot: Pilot | Prefile,
  broker: BrokerAsPromised
) {
  const key = `${pilot.cid}-${pilot.callsign}`;
  const cacheEntry = FLIGHT_PLAN_CACHE[key];
  const flightPlan = pilot.flight_plan;

  if (cacheEntry) {
    const changes = detectFlightPlanChanges(cacheEntry.value, flightPlan);
    if (changes) {
      logger.debug(`Flight Plan ${key} has changed. Publishing changes.`);
      await broker.publish("events.flight_plan.update", {
        event: "update",
        data: {
          original: cacheEntry.value,
          updated: flightPlan,
          updates: changes,
        },
        timestamp: Date.now(),
      });

      FLIGHT_PLAN_CACHE[key] = {
        timestamp: Date.now(),
        value: flightPlan,
      };
    }
  } else {
    logger.debug(`Flight Plan ${key} is not in cache. Adding to cache.`);
    FLIGHT_PLAN_CACHE[key] = {
      timestamp: Date.now(),
      value: flightPlan,
    };

    await broker.publish("events.flight_plan.file", {
      event: "file",
      data: flightPlan,
      timestamp: Date.now(),
    });
  }
}

async function checkForInactiveFlightPlans(broker: BrokerAsPromised) {
  const now = Date.now();
  for (const key in FLIGHT_PLAN_CACHE) {
    const cacheEntry = FLIGHT_PLAN_CACHE[key];
    if (now - cacheEntry.timestamp > INACTIVE_FLIGHT_PLAN_TIMEOUT) {
      logger.debug(`Flight Plan ${key} is inactive. Removing from cache.`);

      delete FLIGHT_PLAN_CACHE[key];

      await broker.publish("events.flight_plan.expire", {
        event: "expire",
        data: cacheEntry.value,
        timestamp: now,
      });
    }
  }
}

function detectFlightPlanChanges(original: FlightPlan, updated: FlightPlan) {
  const changes = pick(
    updated,
    Object.keys(updated).filter(
      (key) =>
        original[key as keyof FlightPlan] !== updated[key as keyof FlightPlan]
    )
  );

  return Object.keys(changes).length > 0 ? changes : null;
}

type Cache<T> = Record<string, { timestamp: number; value: T }>;

export type Pilot = {
  cid: number;
  name: string;
  callsign: string;
  server: string;
  pilot_rating: number;
  military_rating: number;
  latitude: number;
  longitude: number;
  altitude: number;
  groundspeed: number;
  transponder: string;
  heading: number;
  qnh_i_hg: number;
  qnh_mb: number;
  flight_plan: FlightPlan;
  logon_time: string;
  last_updated: string;
};

export type Prefile = {
  cid: number;
  name: string;
  callsign: string;
  flight_plan: FlightPlan;
  last_updated: string;
};

export type FlightPlan = {
  flight_rules: string;
  aircraft: string;
  aircraft_faa: string;
  aircraft_short: string;
  departure: string;
  arrival: string;
  alternate: string;
  cruise_tas: string;
  altitude: string;
  deptime: string;
  enroute_time: string;
  fuel_time: string;
  remarks: string;
  route: string;
  revision_id: number;
  assigned_transponder: string;
};
/**
 *
 */
