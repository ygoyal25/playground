const parquet = require('parquetjs');
const { faker } = require('@faker-js/faker');

const schema = new parquet.ParquetSchema({
    name: { type: 'UTF8' },
    companyName: { type: 'UTF8' },
    experience: { type: 'INT64' },
    salary: { type: 'INT64' },
    date: { type: 'TIMESTAMP_MILLIS' }
});

async function createParquet() {
    try {
        let j = 0;
        while(j < 1) {
            j++;
            const companyName = faker.company.companyName().split(' ')[0].replace(',', '');
            console.log(companyName);
            const writer = await parquet.ParquetWriter.openFile(schema, `employees-${companyName}.parquet`);
            let i = 1000000 * 100;
            while(i >= 0) {
                i--;
                const data = {
                    name: faker.name.firstName(),
                    companyName,
                    experience: 10 * (Math.random()),
                    salary: 30 * (Math.random()),
                    date: new Date()
                };
                console.log(i);
                await writer.appendRow(data);
            }
            writer.close();
        }
        return;
    } catch(e) {
        console.log(e);
    }
}

async function main() {
    await createParquet();
    console.log('I am here');
}

main()