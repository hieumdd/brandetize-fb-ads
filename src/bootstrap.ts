import { getLogger } from './logging.service';
import { createExternalTable } from './google-cloud/bigquery.service';
import * as pipelines from './pipeline/pipeline.const';
import { DATASET, getBucketName } from './config';

const logger = getLogger(__filename);

(async () => {
    const bucketName = await getBucketName();

    await Promise.all([
        ...Object.values(pipelines).map(async (pipeline) => {
            const basePath = `${bucketName}/insights/${pipeline.name}`;

            return await createExternalTable(DATASET, {
                name: pipeline.name,
                schema: pipeline.schema,
                sourceUris: [`gs://${basePath}/*.json`],
                sourceUriPrefix: `gs://${basePath}/{_account_id:INT64}/{_date_start:DATE}`,
            });
        }),
        createExternalTable(DATASET, {
            name: 'Accounts',
            schema: [
                { name: 'account_name', type: 'STRING' },
                { name: 'account_id', type: 'INT64' },
            ],
            sourceUris: [`gs://${bucketName}/accounts.json`],
        }),
    ])
        .then(() => {
            logger.info('Create external tables successfully');
            process.exit(0);
        })
        .catch((error) => {
            logger.error('Create external tables failed', error);
            process.exit(1);
        });
})();
