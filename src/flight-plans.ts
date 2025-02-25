import { BrokerAsPromised, SubscriberSessionAsPromised } from "rascal";
import logger from "./logger";
import { redisService } from "./redis";

export async function addFlightPlanListener(
  subscriptions: SubscriberSessionAsPromised[],
  broker: BrokerAsPromised
) {
  // Initialize Redis and subscribe to expiration events
  await redisService.connect();
  await redisService.subscribeToExpiration(async (key) => {
    const [cid, callsign] = key.split("-");
    logger.debug(`Flight Plan ${key} expired. Publishing expiration event.`);

    const expiredPlan = await redisService.getFlightPlan(key);
    if (expiredPlan) {
      await publishFlightPlanEvent(
        {
          event: "expire",
          pilot: expiredPlan.pilot,
          flight_plan: expiredPlan.flightPlan,
          timestamp: Date.now(),
        },
        broker
      );
    }
  });

  for (const subscription of subscriptions) {
    subscription.on("message", async (_: any, content: any, ackOrNack: any) => {
      await processRawFlightPlan(content?.data, broker);
      await ackOrNack();
    });
  }
}

export async function processRawFlightPlan(
  pilot: Pilot | Prefile,
  broker: BrokerAsPromised
) {
  const key = `${pilot.cid}-${pilot.callsign}`;
  const existingData = await redisService.getFlightPlan(key);
  const flightPlan = pilot.flight_plan;

  if (existingData) {
    const hasChanges = detectFlightPlanChanges(
      existingData.flightPlan,
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

      await redisService.setFlightPlan(key, {
        timestamp: Date.now(),
        pilot: { cid: pilot.cid, callsign: pilot.callsign },
        flightPlan,
      });
    } else {
      // Just refresh the TTL since we saw this flight plan in the feed
      await redisService.refreshTTL(key);
    }
  } else {
    logger.debug(`Flight Plan ${key} is not in cache. Adding to cache.`);
    await redisService.setFlightPlan(key, {
      timestamp: Date.now(),
      pilot: { cid: pilot.cid, callsign: pilot.callsign },
      flightPlan,
    });

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
