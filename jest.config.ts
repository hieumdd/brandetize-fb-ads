export default {
    preset: 'ts-jest',
    testEnvironment: 'node',
    testRegex: './src/.*.test.ts$',
    setupFiles: ['dotenv/config'],
    testTimeout: 540000,
};
