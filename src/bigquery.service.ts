import { BigQuery } from '@google-cloud/bigquery';

import { getLogger } from './logging.service';

const logger = getLogger(__filename);

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
        .on('job', () => {
            getLogger(__filename).debug({ dataset: DATASET, table, schema: options.schema });
        });
};

type CreateExternalTableOptions = {
    name: string;
    sourceUris: string[];
    schema: any[];
    sourceUriPrefix?: string;
};

export const createExternalTable = async (dataset: string, options: CreateExternalTableOptions) => {
    const { name: tableName, sourceUris, schema, sourceUriPrefix } = options;

    const table = client.dataset(dataset).table(tableName);
    if (await table.exists().then(([response]) => response)) {
        logger.debug(`table ${table.id} exists, replacing`);
        await table.delete();
    }
    await table.create({
        schema,
        externalDataConfiguration: {
            sourceUris,
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            ignoreUnknownValues: true,
            hivePartitioningOptions: sourceUriPrefix
                ? { mode: 'CUSTOM', sourceUriPrefix }
                : undefined,
        },
    });
};
