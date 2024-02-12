import { createLogger, format, transports } from 'winston';
import stringify from 'safe-stable-stringify';

export const logger = createLogger({
    level: 'debug',
    format: format.printf(({ message, level, ...meta }) => {
        const _message = message instanceof Object ? message : { message };
        return stringify({ severity: level, ..._message, ...meta })!;
    }),
    transports: [new transports.Console()],
});
