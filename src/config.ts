import "dotenv/config";

export const config = {
  REFRESH_INTERVAL_MS: Number.parseInt(process.env.REFRESH_INTERVAL_MS!),
  RABBIT_URL: process.env.RABBIT_URL!,
  LOG_LEVEL: process.env.LOG_LEVEL!,
  VATSIM_DATA_URL: process.env.VATSIM_DATA_URL!,
};
