import { Storage } from '@google-cloud/storage';

const storage = new Storage();
const bucket = storage.bucket('facebook-578489313388');

export const createWriteStream = (name: string) => {
    return bucket.file(name).createWriteStream();
};
