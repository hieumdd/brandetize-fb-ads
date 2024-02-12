import { BigQuery } from '@google-cloud/bigquery';
import { logger } from './logging.service';

const client = new BigQuery();

const DATASET = 'Facebook';

export type CreateLoadStreamConfig = {
    schema: Record<string, any>[];
    writeDisposition: 'WRITE_APPEND' | 'WRITE_TRUNCATE';
};

export const createLoadStream = (options: CreateLoadStreamConfig, table: string) => {
    return client
        .dataset(DATASET)
        .table(table)
        .createWriteStream({
            schema: { fields: options.schema },
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            createDisposition: 'CREATE_IF_NEEDED',
            writeDisposition: options.writeDisposition,
        })
        .on('job', () => logger.debug({ fn: 'bigquery.service:load', table }));
};
