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

    const expiredPlan = (await redisService.getFlightPlan(
      key
    )) as RedisFlightPlan;
    if (expiredPlan) {
      // First publish the state change to cancelled
      await publishFlightPlanEvent(
        {
          event: "state_change",
          pilot: expiredPlan.pilot,
          flight_plan: expiredPlan.flightPlan,
          timestamp: Date.now(),
          state: {
            previous: expiredPlan.state,
            current: "cancelled",
            reason: "flight_plan_expired",
          },
        },
        broker
      );

      // Then publish the expire event
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
    const data = (await redisService.getFlightPlan(
      existingKey
    )) as RedisFlightPlan;
    if (data && data.flightPlan.departure === flightPlan.departure) {
      existingMatchFound = true;

      // If the flight plan has changes, update it
      if (detectFlightPlanChanges(data.flightPlan, flightPlan)) {
        logger.debug(`Flight Plan ${existingKey} has changed. Updating.`);
        await redisService.setFlightPlan(existingKey, {
          timestamp: Date.now(),
          pilot: { cid: pilot.cid, callsign: pilot.callsign },
          flightPlan,
          state: data.state,
          lastStateChange: data.lastStateChange,
          previousAltitude: data.previousAltitude,
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
      }

      // Check for state changes if we have pilot position data
      if ("latitude" in pilot) {
        logger.debug(
          `Checking state change for ${existingKey}. Current state: ${data.state}, Ground speed: ${pilot.groundspeed}, Altitude: ${pilot.altitude}`
        );
        const stateChange = determineStateChange(
          data.state,
          pilot,
          data.previousAltitude
        );

        if (stateChange) {
          logger.debug(
            `State change determined: ${data.state} -> ${stateChange.newState} (${stateChange.reason})`
          );
          await updateFlightPlanState(
            existingKey,
            data,
            stateChange.newState,
            stateChange.reason,
            pilot,
            broker
          );
        } else {
          logger.debug(`No state change needed for ${existingKey}`);
        }

        // Only update previous altitude if no state change occurred
        if (!stateChange) {
          await redisService.setFlightPlan(existingKey, {
            ...data,
            previousAltitude: pilot.altitude,
            timestamp: Date.now(),
          });
        }
      }

      // Refresh TTL regardless of changes
      await redisService.refreshTTL(existingKey);
      break;
    }
  }

  // If no matching flight plan found, expire old ones and create new
  if (!existingMatchFound) {
    // Expire any existing flight plans for this pilot/callsign
    for (const existingKey of existingKeys) {
      const data = (await redisService.getFlightPlan(
        existingKey
      )) as RedisFlightPlan;
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
      state: "filed",
      lastStateChange: Date.now(),
      previousAltitude: "latitude" in pilot ? pilot.altitude : undefined,
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

type FlightPlanState =
  | "filed" // Initial state when flight plan is filed
  | "departing" // Pilot is connected and at departure airport
  | "enroute" // Pilot is in flight
  | "approaching" // Pilot is approaching destination
  | "arrived" // Flight completed
  | "cancelled"; // Flight cancelled/expired

// Define valid state transitions
const validStateTransitions: Record<FlightPlanState, FlightPlanState[]> = {
  filed: ["departing", "enroute", "cancelled"], // Allow direct transition to enroute
  departing: ["enroute", "cancelled"],
  enroute: ["approaching", "arrived", "cancelled"], // Allow direct transition to arrived
  approaching: ["arrived", "cancelled"],
  arrived: [], // Terminal state
  cancelled: [], // Terminal state
};

// Constants for state transitions
const GROUND_SPEED_THRESHOLDS = {
  TAXI: 30, // knots - typical max taxi speed
  TAKEOFF: 60, // knots - typical rotation speed
  LANDING: 60, // knots - typical approach speed
} as const;

const ALTITUDE_THRESHOLDS = {
  GROUND: 100, // feet - considered on the ground
  CLIMBING: 1000, // feet - typical initial climb
  DESCENDING: 1000, // feet - typical initial descent
} as const;

function determineStateChange(
  currentState: FlightPlanState | undefined, // Allow undefined for initial state
  pilot: Pilot,
  previousAltitude?: number
): { newState: FlightPlanState; reason: string } | null {
  // If no current state, treat as "filed"
  if (!currentState) {
    currentState = "filed";
  }

  // Skip state changes for terminal states
  if (currentState === "arrived" || currentState === "cancelled") {
    logger.debug(`Skipping state change for terminal state: ${currentState}`);
    return null;
  }

  const { groundspeed, altitude } = pilot;
  logger.debug(
    `Determining state change from ${currentState}. Ground speed: ${groundspeed}, Altitude: ${altitude}`
  );

  switch (currentState) {
    case "filed":
      // If pilot is already airborne, consider them enroute
      if (groundspeed > GROUND_SPEED_THRESHOLDS.TAKEOFF) {
        logger.debug(
          `Filed -> Enroute transition: Ground speed ${groundspeed} > ${GROUND_SPEED_THRESHOLDS.TAKEOFF}`
        );
        return {
          newState: "enroute",
          reason: "already_airborne",
        };
      }
      // If pilot is connected and at low speed, consider them departing
      if (groundspeed < GROUND_SPEED_THRESHOLDS.TAXI) {
        logger.debug(
          `Filed -> Departing transition: Ground speed ${groundspeed} < ${GROUND_SPEED_THRESHOLDS.TAXI}`
        );
        return {
          newState: "departing",
          reason: "pilot_connected_at_gate",
        };
      }
      break;

    case "departing":
      // If ground speed increases significantly, consider them enroute
      if (groundspeed > GROUND_SPEED_THRESHOLDS.TAKEOFF) {
        logger.debug(
          `Departing -> Enroute transition: Ground speed ${groundspeed} > ${GROUND_SPEED_THRESHOLDS.TAKEOFF}`
        );
        return {
          newState: "enroute",
          reason: "ground_speed_above_takeoff_threshold",
        };
      }
      break;

    case "enroute":
      // If ground speed is very low, consider them arrived
      if (groundspeed < GROUND_SPEED_THRESHOLDS.TAXI) {
        logger.debug(
          `Enroute -> Arrived transition: Ground speed ${groundspeed} < ${GROUND_SPEED_THRESHOLDS.TAXI}`
        );
        return {
          newState: "arrived",
          reason: "already_landed",
        };
      }
      // If ground speed is reducing, consider them approaching
      if (groundspeed < GROUND_SPEED_THRESHOLDS.LANDING) {
        logger.debug(
          `Enroute -> Approaching transition: Ground speed ${groundspeed} < ${GROUND_SPEED_THRESHOLDS.LANDING}`
        );
        return {
          newState: "approaching",
          reason: "slowing_for_approach",
        };
      }
      break;

    case "approaching":
      // If ground speed is very low, consider them arrived
      if (groundspeed < GROUND_SPEED_THRESHOLDS.TAXI) {
        logger.debug(
          `Approaching -> Arrived transition: Ground speed ${groundspeed} < ${GROUND_SPEED_THRESHOLDS.TAXI}`
        );
        return {
          newState: "arrived",
          reason: "landed_and_taxiing",
        };
      }
      break;
  }

  logger.debug(`No state change needed for ${currentState}`);
  return null;
}

type RedisFlightPlan = {
  timestamp: number;
  pilot: {
    cid: number;
    callsign: string;
  };
  flightPlan: FlightPlan;
  state: FlightPlanState;
  lastStateChange: number;
  previousAltitude?: number; // Store previous altitude for tracking changes
};

type FlightPlanEvent = {
  event: "file" | "update" | "expire" | "state_change";
  pilot: {
    cid: number;
    callsign: string;
  };
  flight_plan: FlightPlan;
  timestamp: number;
  state?: {
    previous: FlightPlanState;
    current: FlightPlanState;
    reason: string;
  };
  position?: {
    latitude: number;
    longitude: number;
    altitude: number;
    groundspeed: number;
    heading: number;
  };
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

function isValidStateTransition(
  currentState: FlightPlanState,
  newState: FlightPlanState
): boolean {
  return validStateTransitions[currentState].includes(newState);
}

async function updateFlightPlanState(
  key: string,
  currentData: RedisFlightPlan,
  newState: FlightPlanState,
  reason: string,
  pilot: Pilot | Prefile,
  broker: BrokerAsPromised
) {
  // If no current state, treat as "filed"
  const currentState = currentData.state || "filed";

  logger.debug(
    `Checking state transition from ${currentState} to ${newState} for flight plan ${key}`
  );
  logger.debug(
    `Valid transitions from ${currentState}: ${validStateTransitions[
      currentState
    ].join(", ")}`
  );

  if (!isValidStateTransition(currentState, newState)) {
    logger.debug(
      `Invalid state transition from ${currentState} to ${newState} for flight plan ${key}`
    );
    return;
  }

  const previousState = currentState;
  const now = Date.now();

  // Update Redis data
  await redisService.setFlightPlan(key, {
    ...currentData,
    state: newState,
    lastStateChange: now,
    timestamp: now,
  });

  // Publish state change event
  await publishFlightPlanEvent(
    {
      event: "state_change",
      pilot: { cid: pilot.cid, callsign: pilot.callsign },
      flight_plan: currentData.flightPlan,
      timestamp: now,
      state: {
        previous: previousState,
        current: newState,
        reason,
      },
      position:
        "latitude" in pilot
          ? {
              latitude: pilot.latitude,
              longitude: pilot.longitude,
              altitude: pilot.altitude,
              groundspeed: pilot.groundspeed,
              heading: pilot.heading,
            }
          : undefined,
    },
    broker
  );

  logger.debug(
    `Flight plan ${key} state changed from ${previousState} to ${newState}: ${reason}`
  );
}
/**
 *
 */
