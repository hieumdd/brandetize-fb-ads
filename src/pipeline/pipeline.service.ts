import path from 'node:path';
import { Readable, Transform, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import ndjson from 'ndjson';

import dayjs from '../dayjs';
import { getLogger } from '../logging.service';
import { createLoadStream } from '../bigquery.service';
import { createWriteStream } from '../storage.service';
import { createTasks } from '../cloud-tasks.service';
import { getAccounts } from '../facebook/account.service';
import { CreatePipelineTasksBody, PipelineOptions } from './pipeline.request.dto';
import * as pipelines from './pipeline.const';

const logger = getLogger(__filename);

export const runPipeline = async (pipeline_: pipelines.Pipeline, options: PipelineOptions) => {
    logger.info('pipeline start', { pipeline: pipeline_.name, options });

    const transform = () => {
        return new Transform({
            objectMode: true,
            transform: (row, _, callback) => {
                const batchedAt = { _batched_at: dayjs().utc().toISOString() };
                pipeline_.validationSchema
                    .validateAsync(row)
                    .then((value) => callback(null, { ...value, ...batchedAt }))
                    .catch((error) => callback(error));
            },
        });
    };

    const groupBy = () => {
        const state: Record<string, object[]> = {};
        return new Transform({
            objectMode: true,
            transform(row, _, callback) {
                state[row.date_start] = [...(state[row.date_start] ?? []), row];
                callback();
            },
            flush(callback) {
                Object.entries(state).forEach((rows) => this.push(rows));
                callback();
            },
        });
    };

    const write = () => {
        return new Writable({
            objectMode: true,
            write: ([key, rows], _, callback) => {
                const name = path.join(
                    'ads-insights',
                    `_account_id=${options.accountId}`,
                    `_date_start=${key}`,
                    'data.json',
                );
                pipeline(Readable.from(rows), ndjson.stringify(), createWriteStream(name))
                    .then(() => callback())
                    .catch((error) => callback(error));
            },
        });
    };

    return pipeline(
        await pipeline_.getExtractStream(options),
        transform(),
        groupBy(),
        write(),
    ).then(() => options);
};

export const createInsightsPipelineTasks = async ({ start, end }: CreatePipelineTasksBody) => {
    logger.info('creating insights pipeline tasks', { start, end });

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
