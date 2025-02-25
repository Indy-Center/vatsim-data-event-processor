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
    const [cid, callsign, departure, timestamp] = key.split("-");
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
      if (
        content?.data?.flight_plan &&
        content?.data?.flight_plan?.flight_rules === "I"
      ) {
        await processRawFlightPlan(content?.data, broker);
      }

      await ackOrNack();
    });
  }
}

export async function processRawFlightPlan(
  pilot: Pilot | Prefile,
  broker: BrokerAsPromised
) {
  const baseKey = `${pilot.cid}-${pilot.callsign}`;
  const flightPlan = pilot.flight_plan;

  // Get all existing keys for this pilot/callsign combination
  const existingKeys = await redisService.findKeysByPattern(`${baseKey}*`);
  let existingMatchFound = false;

  // First check for an exact match (same CID, callsign, and departure)
  for (const existingKey of existingKeys) {
    const data = await redisService.getFlightPlan(existingKey);
    if (data && data.flightPlan.departure === flightPlan.departure) {
      existingMatchFound = true;

      // If the flight plan has changes, update it
      if (detectFlightPlanChanges(data.flightPlan, flightPlan)) {
        logger.debug(`Flight Plan ${existingKey} has changed. Updating.`);
        await redisService.setFlightPlan(existingKey, {
          timestamp: Date.now(),
          pilot: { cid: pilot.cid, callsign: pilot.callsign },
          flightPlan,
        });

        await publishFlightPlanEvent(
          {
            event: "update",
            pilot: { cid: pilot.cid, callsign: pilot.callsign },
            flight_plan: flightPlan,
            timestamp: Date.now(),
          },
          broker
        );
      } else {
        // Just refresh TTL if no changes
        logger.debug(
          `Flight Plan ${existingKey} still active. Refreshing TTL.`
        );
        await redisService.refreshTTL(existingKey);
      }
      break;
    }
  }

  // If no matching flight plan found, expire old ones and create new
  if (!existingMatchFound) {
    // Expire any existing flight plans for this pilot/callsign
    for (const existingKey of existingKeys) {
      const data = await redisService.getFlightPlan(existingKey);
      if (data) {
        logger.debug(`Expiring previous flight plan ${existingKey}`);
        await publishFlightPlanEvent(
          {
            event: "expire",
            pilot: data.pilot,
            flight_plan: data.flightPlan,
            timestamp: Date.now(),
          },
          broker
        );
        await redisService.deleteFlightPlan(existingKey);
      }
    }

    // Create new flight plan
    const key = `${baseKey}-${flightPlan.departure}`;
    logger.debug(`Creating new flight plan ${key}`);
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
  // Handle null or undefined cases
  if (!original || !updated) return true;

  return Object.keys(updated).some((key) => {
    const field = key as keyof FlightPlan;
    // Compare as strings to handle numeric values that might be stored as strings
    return String(original[field]) !== String(updated[field]);
  });
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

export async function debugFlightPlanTTL(key: string) {
  const flightPlan = await redisService.getFlightPlan(key);
  if (flightPlan) {
    const ttl = await redisService.getRemainingTTL(key);
    logger.debug(
      `Flight Plan ${key} TTL: ${ttl}s, Last updated: ${new Date(
        flightPlan.timestamp
      ).toISOString()}`
    );
    return { flightPlan, ttl };
  }
  logger.debug(`Flight Plan ${key} not found in cache`);
  return null;
}
/**
 *
 */
