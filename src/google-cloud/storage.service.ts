import { Storage } from '@google-cloud/storage';

const storage = new Storage();

export const createWriteStream = (bucketName: string, fileName: string) => {
    return storage.bucket(bucketName).file(fileName).createWriteStream();
};
