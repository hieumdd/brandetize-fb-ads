import { getAccounts } from './account.service';

it('getAccounts', async () => {
    return await getAccounts(618162358531378)
        .then((result) => expect(result).toBeDefined())
        .catch((error) => {
            console.error({ error });
            throw error;
        });
});
