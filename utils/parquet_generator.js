const parquet = require('parquetjs');
const { faker } = require('@faker-js/faker');

const schema = new parquet.ParquetSchema({
    file: { type: 'UTF8', compression: 'SNAPPY' },
    extension: { type: 'UTF8', compression: 'SNAPPY' },
    fileType: { type: 'INT32', compression: 'SNAPPY' },
    changeType: { type: 'INT32', compression: 'SNAPPY' },
    inode: { type: 'INT64', compression: 'SNAPPY' },
    size: { type: 'INT64', compression: 'SNAPPY' },
    ctime: { type: 'TIMESTAMP_MILLIS', compression: 'SNAPPY' },
    mtime: { type: 'TIMESTAMP_MILLIS', compression: 'SNAPPY' },
    crtime: { type: 'TIMESTAMP_MILLIS', compression: 'SNAPPY' }
});

async function createParquet() {
    try {
        let j = 0;
        while(j < 1) {
            j++;
            const companyName = faker.company.companyName().split(' ')[0].replace(',', '');
            console.log(companyName);
            const writer = await parquet.ParquetWriter.openFile(schema, `employees-${companyName}.parquet`);
            let i = 10 * 10;
            while(i >= 0) {
                i--;
                const data = {
                    file: `/${faker.name.firstName()}`,
                    extension: ['log', 'txt', 'sh'][Math.floor(Math.random() * 3)],
                    fileType: Math.random() > 0.5 ? 0 : 1,
                    changeType: Math.random() > 0.5 ? 1 : 2,
                    inode: Math.floor(2000 * Math.random()),
                    size: Math.floor(100*Math.random()),
                    ctime: (new Date()).toISOString(),
                    mtime: (new Date()).toISOString(),
                    crtime: (new Date()).toISOString()
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

module.exports = createParquet;

