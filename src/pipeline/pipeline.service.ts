import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import ndjson from 'ndjson';

import dayjs from '../dayjs';
import { logger } from '../logging.service';
import { createLoadStream } from '../bigquery.service';
import { createTasks } from '../cloud-tasks.service';
import { getAccounts } from '../facebook/account.service';
import { CreatePipelineTasksBody, PipelineOptions } from './pipeline.request.dto';
import * as pipelines from './pipeline.const';

export const runPipeline = async (pipeline_: pipelines.Pipeline, options: PipelineOptions) => {
    logger.info({ fn: 'pipeline.service:runPipeline', pipeline: pipeline_.name, options });

    return pipeline(
        await pipeline_.getExtractStream(options),
        new Transform({
            objectMode: true,
            transform: (row, _, callback) => {
                const batchedAt = { _batched_at: dayjs().utc().toISOString() };
                pipeline_.validationSchema
                    .validateAsync(row)
                    .then((value) => callback(null, { ...value, ...batchedAt }))
                    .catch((error) => callback(error));
            },
        }),
        ndjson.stringify(),
        createLoadStream(
            {
                schema: [
                    ...pipeline_.loadConfig.schema,
                    { name: '_batched_at', type: 'TIMESTAMP' },
                ],
                writeDisposition: pipeline_.loadConfig.writeDisposition,
            },
            `p_${pipeline_.name}__${options.accountId}`,
        ),
    ).then(() => options);
};

export const createInsightsPipelineTasks = async ({ start, end }: CreatePipelineTasksBody) => {
    logger.info({ fn: 'pipeline.service:createInsightsPipelineTasks', options: { start, end } });

    const accounts = await getAccounts(618162358531378);

    return await Promise.all([
        Object.keys(pipelines)
            .map((pipeline) => {
                return accounts.map(({ account_id }) => ({
                    accountId: account_id,
                    start,
                    end,
                    pipeline,
                }));
            })
            .map((data) => createTasks(data, (task) => [task.pipeline, task.accountId].join('-'))),
        pipeline(
            Readable.from(accounts),
            ndjson.stringify(),
            createLoadStream(
                {
                    schema: [
                        { name: 'account_name', type: 'STRING' },
                        { name: 'account_id', type: 'INT64' },
                    ],
                    writeDisposition: 'WRITE_TRUNCATE',
                },
                'Accounts',
            ),
        ),
    ]).then(() => accounts.length);
};
