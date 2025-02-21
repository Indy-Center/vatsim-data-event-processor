import winston from "winston";
import { config } from "./config";

const logger = winston.createLogger({
  level: config.LOG_LEVEL,
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      ),
    }),
  ],
});

export default logger;
