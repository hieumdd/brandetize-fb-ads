import { getProjectNumber } from './resource-manager.service';

export const getBucketName = async () => {
    const projectNumber = await getProjectNumber();
    return `facebook-${projectNumber}`;
};

export const DATASET = 'FacebookTesting';
