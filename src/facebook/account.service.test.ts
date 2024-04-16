import { getAccounts } from './account.service';

it('getAccounts', async () => {
    return await getAccounts('10152454842563460')
        .then((result) => {
            const x = result.find((account) => account.account_id === '408833080723726');
            expect(result).toBeDefined();
        })
        .catch((error) => {
            console.error({ error });
            throw error;
        });
});
