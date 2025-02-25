import { BrokerAsPromised, SubscriberSessionAsPromised } from "rascal";
import logger from "./logger";
import { pick } from "lodash";

const INACTIVE_FLIGHT_PLAN_TIMEOUT = 1000 * 60 * 10; // 10 minutes
const INACTIVE_FLIGHT_PLAN_CHECK_INTERVAL = 1000 * 60; // 1 minute
const FLIGHT_PLAN_CACHE: Cache<{
  pilot: { cid: number; callsign: string };
  flightPlan: FlightPlan;
}> = {};

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

  // Check for inactive flight plans periodically
  setInterval(async () => {
    logger.debug("Checking for inactive flight plans");
    await checkForInactiveFlightPlans(broker);
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
    const hasChanges = detectFlightPlanChanges(
      cacheEntry.value.flightPlan,
      flightPlan
    );
    if (hasChanges) {
      logger.debug(`Flight Plan ${key} has changed. Publishing update.`);
      await publishFlightPlanEvent(
        {
          event: "update",
          pilot: { cid: pilot.cid, callsign: pilot.callsign },
          flight_plan: flightPlan,
          timestamp: Date.now(),
        },
        broker
      );

      FLIGHT_PLAN_CACHE[key] = {
        timestamp: Date.now(),
        value: {
          pilot: { cid: pilot.cid, callsign: pilot.callsign },
          flightPlan,
        },
      };
    } else {
      FLIGHT_PLAN_CACHE[key].timestamp = Date.now();
    }
  } else {
    logger.debug(`Flight Plan ${key} is not in cache. Adding to cache.`);
    FLIGHT_PLAN_CACHE[key] = {
      timestamp: Date.now(),
      value: {
        pilot: { cid: pilot.cid, callsign: pilot.callsign },
        flightPlan,
      },
    };

    await publishFlightPlanEvent(
      {
        event: "file",
        pilot: { cid: pilot.cid, callsign: pilot.callsign },
        flight_plan: flightPlan,
        timestamp: Date.now(),
      },
      broker
    );
  }
}

async function checkForInactiveFlightPlans(broker: BrokerAsPromised) {
  const now = Date.now();
  for (const key in FLIGHT_PLAN_CACHE) {
    const cacheEntry = FLIGHT_PLAN_CACHE[key];
    if (now - cacheEntry.timestamp > INACTIVE_FLIGHT_PLAN_TIMEOUT) {
      logger.debug(`Flight Plan ${key} is inactive. Removing from cache.`);

      delete FLIGHT_PLAN_CACHE[key];

      await publishFlightPlanEvent(
        {
          event: "expire",
          pilot: cacheEntry.value.pilot,
          flight_plan: cacheEntry.value.flightPlan,
          timestamp: now,
        },
        broker
      );
    }
  }
}

async function publishFlightPlanEvent(
  event: FlightPlanEvent,
  broker: BrokerAsPromised
) {
  await broker.publish(`events.flight_plan.${event.event}`, event);
}

function detectFlightPlanChanges(original: FlightPlan, updated: FlightPlan) {
  return Object.keys(updated).some(
    (key) =>
      original[key as keyof FlightPlan] !== updated[key as keyof FlightPlan]
  );
}

type Cache<T> = Record<string, { timestamp: number; value: T }>;

type FlightPlanEvent = {
  event: "file" | "update" | "expire";
  pilot: {
    cid: number;
    callsign: string;
  };
  flight_plan: FlightPlan;
  timestamp: number;
};

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
