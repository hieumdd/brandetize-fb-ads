import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import ndjson from 'ndjson';

import { getLogger } from '../logging.service';
import { getProjectNumber } from '../resource-manager.service';
import { createWriteStream } from '../storage.service';
import { createTasks } from '../cloud-tasks.service';
import { getAccounts } from '../facebook/account.service';
import { CreatePipelineTasksBody, FacebookRequestOptions } from './pipeline.request.dto';
import { RunPipelineOptions } from './pipeline.utils';
import * as pipelines from './pipeline.const';

const logger = getLogger(__filename);

const getBucketName = async () => {
    const projectNumber = await getProjectNumber();
    return `facebook-${projectNumber}`;
};

type RunInsightsPipelineConfig = {
    name: string;
    run: (options: RunPipelineOptions) => Promise<any>;
};

export const runInsightsPipeline = async (
    { name, run }: Omit<RunInsightsPipelineConfig, 'bucketName'>,
    options: FacebookRequestOptions,
) => {
    logger.info(`Running insights pipeline ${name}`, options);

    const bucketName = await getBucketName();

    return await run({ ...options, bucketName }).then(() => options);
};

export const createInsightsPipelineTasks = async ({ start, end }: CreatePipelineTasksBody) => {
    logger.info('Creating insights pipeline tasks', { start, end });

    const bucketName = await getBucketName();
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
            createWriteStream(bucketName, 'accounts.json'),
        ),
    ]).then(() => accounts.length);
};
