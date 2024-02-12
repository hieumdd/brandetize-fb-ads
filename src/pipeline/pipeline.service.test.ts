import * as pipelines from './pipeline.const';
import { createInsightsPipelineTasks, runPipeline } from './pipeline.service';

describe('pipeline', () => {
    it.each([pipelines.ADS_INSIGHTS])(
        '$name',
        async (pipeline) => {
            return await runPipeline(pipeline, {
                accountId: '662385836106323',
                start: '2023-12-01',
                end: '2024-12-01',
            })
                .then((results) => expect(results).toBeDefined())
                .catch((error) => {
                    console.error({ error });
                    throw error;
                });
        },
        100_000_000,
    );
});

it('create-tasks-insights', async () => {
    return await createInsightsPipelineTasks({
        start: '2023-08-28',
        end: '2023-09-04',
    })
        .then((result) => expect(result).toBeDefined())
        .catch((error) => {
            console.error({ error });
            throw error;
        });
});
