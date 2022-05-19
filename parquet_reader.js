const fs = require('fs');
const parquet = require('parquetjs');
const { connect, queryTable } = require('./synapse_connect');
const { Request } = require('tedious');

const schema = new parquet.ParquetSchema({
    file: { type: 'UTF8' },
    companyName: { type: 'UTF8' },
    experience: { type: 'INT64' },
    salary: { type: 'INT64' },
    date: { type: 'TIMESTAMP_MILLIS' }
});

const filesArray = [];

function recursiveRead(file = '/Users/yashendra/Downloads/changes') {
    try {
        let files = fs.readdirSync(file)
        // console.log(files);
        if (files) {
            files = files.filter(item => !(/(^|\/)\.[^\/\.]/g).test(item));
            // console.log(files);
            files.forEach(f => recursiveRead(file + '/' + f));
        }
    } catch(e) {
        // console.log(e);
        filesArray.push(file);
    }
}


recursiveRead()

// console.log(filesArray);

function formatData(name, value) {
    switch(name) {
        case 'file':
        case 'extension':
        case 'workingEnvironmentId':
        case 'volumeId':
        case 'snapshotId':
            return `'${value}'`
        case 'inode':
        case 'size':
        case 'fileType':
        case 'changeType': 
            return value
        case 'ctime':
        case 'mtime':
        case 'crtime':
            return `'${(new Date(value)).toISOString()}'`
        default:
            return value
    }
}


async function main() {
    const connection = await connect();
    for(var i = 0; i < filesArray.length; i++) {
        const file = filesArray[i];
    
        let reader = await parquet.ParquetReader.openFile(file);
        
        // create a new cursor
        let cursor = reader.getCursor();
        
        // read all records from the file and print them
        let record = null;
        console.log(file)
        const workingenvironmentid = file.split('/volumeid')[0].split('workingenvironmentid=')[1];
        console.log(workingenvironmentid);
        const volumeId = file.split('/snapshotid')[0].split('/volumeid=')[1];
        const snapshotid = file.split('snapshotid=')[1].split('/')[0];
        const baseQuery = `INSERT INTO dbo.changes_catalog VALUES `;
        // (file, extension, fileType, changeType, inode, size, ctime, mtime, crtime, workingEnvironmentId, volumeId, snapshotId)
        let query = baseQuery;
        while (record = await cursor.next()) {
            // console.log(record);
            query += '('
            for (const x in record) {
                query += formatData(x, record[x]) + ', '
            }
            query += `${formatData('workingEnvironmentId', workingenvironmentid)}, ${formatData('volumeId', volumeId)}, ${formatData('snapshotId', snapshotid)})`
            // console.log(query);
            await execQuery(connection, query);
            query = baseQuery;
        }
        await reader.close();
    }
}
main()
// queryTable()

function execQuery(connection, query) {
    return new Promise((res, rej) => {
        var request = new Request(query, function(err, rowCount, rows) {
            console.log(err, rowCount, rows)
        });
        request.setTimeout(45000);
        connection.execSql(request);
        request.on('requestCompleted', function () {
            // Next SQL statement.
            res(true);
        });
    })
}

// const file = '/Users/yashendra/Downloads/changes/workingenvironmentid=e6f89334-c7a0-11ec-a3cb-a3b5d5547423/volumeid=ffe4b5bf-c85b-11ec-b52d-0fbc82604916/snapshotid=8442ffcf-6a45-4da8-9b1c-903cef55ceec/8442ffcf-6a45-4da8-9b1c-903cef55ceec-changes.parquet'



// console.log(snapshotid);


// let reader = await parquet.ParquetReader.openFile('');
 
// // create a new cursor
// let cursor = reader.getCursor();
 
// // read all records from the file and print them
// let record = null;
// while (record = await cursor.next()) {
//   console.log(record);
// }