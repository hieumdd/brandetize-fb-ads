import Joi from 'joi';
import { ContainerTypes, ValidatedRequestSchema } from 'express-joi-validation';

import dayjs from '../dayjs';
import * as pipelines from './pipeline.const';

export type PipelineOptions = {
    accountId: string;
    start: string;
    end: string;
};

export type CreatePipelineTasksBody = Partial<Omit<PipelineOptions, 'accountId'>>;

export interface CreatePipelineTasksRequest extends ValidatedRequestSchema {
    [ContainerTypes.Body]: CreatePipelineTasksBody;
}

export const CreatePipelineTasksBodySchema = Joi.object<CreatePipelineTasksBody>({
    start: Joi.string()
        .optional()
        .empty(null)
        .allow(null)
        .default(dayjs.utc().subtract(7, 'day').format('YYYY-MM-DD')),
    end: Joi.string().optional().empty(null).allow(null).default(dayjs.utc().format('YYYY-MM-DD')),
});

export type RunPipelineBody = PipelineOptions & { pipeline: keyof typeof pipelines };

export interface RunPipelineRequest extends ValidatedRequestSchema {
    [ContainerTypes.Body]: RunPipelineBody;
}

export const RunPipelineBodySchema = Joi.object<RunPipelineBody>({
    accountId: Joi.string().required(),
    start: Joi.string().required(),
    end: Joi.string().required(),
    pipeline: Joi.string()
        .valid(...Object.keys(pipelines))
        .required(),
});
