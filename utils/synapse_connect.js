const { Connection, Request } = require('tedious');
async function connect() { 
    console.log('Starting...');
    return new Promise((res, rej) => {
        const config = { 
            server: 'yg-synapse-workspace-ondemand.sql.azuresynapse.net',
            authentication: { 
                // type: 'default', 
                type: 'azure-active-directory-default',
                options: { 
                    // token: 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImpTMVhvMU9XRGpfNTJ2YndHTmd2UU8yVnpNYyIsImtpZCI6ImpTMVhvMU9XRGpfNTJ2YndHTmd2UU8yVnpNYyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldC8iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC81MWI3ZjM5MS05M2Q4LTRlZmUtOGYyNi1kNTMyNThhNzc2MTkvIiwiaWF0IjoxNjUyOTM5NDg3LCJuYmYiOjE2NTI5Mzk0ODcsImV4cCI6MTY1Mjk0NTEzMCwiYWNyIjoiMSIsImFpbyI6IkFXUUFtLzhUQUFBQS9PYngxb3BwbVVtYkpYeDZOeHdaVXM3S09vQ1kwL2JhdUNJSkNuME83MTk4YTdzTHNNNnVYZlRSaWFZdGkrQnNPWTJIdDFQOXJ5dlUzQjdVUi95M3JWelJpMGJxMmdaUTZDNUl0M2llMDN5L3N5MWFVeFNIaVBZTkkzZGc3c1V4IiwiYWx0c2VjaWQiOiIxOmxpdmUuY29tOjAwMDM3RkZFMEQ3OUFDNkUiLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiMDRiMDc3OTUtOGRkYi00NjFhLWJiZWUtMDJmOWUxYmY3YjQ2IiwiYXBwaWRhY3IiOiIwIiwiZW1haWwiOiJnb3lhbC55YXNoZW5kcmFAZ21haWwuY29tIiwiZmFtaWx5X25hbWUiOiJHb3lhbCIsImdpdmVuX25hbWUiOiJZYXNoZW5kcmEiLCJncm91cHMiOlsiMmEzYjZhOWQtMzY5Mi00OTIxLWIwYmEtMzI3MWI1N2Q5MWE0Il0sImlkcCI6ImxpdmUuY29tIiwiaXBhZGRyIjoiNDkuMjA3LjE5NC4xMDciLCJuYW1lIjoiWWFzaGVuZHJhIEdveWFsIiwib2lkIjoiMzRjYjE1ZjktNzkzMC00MjE4LThlNGMtNDZiZWNhOTI2NzMwIiwicHVpZCI6IjEwMDMyMDAxRjU2NDA2MTgiLCJyaCI6IjAuQVZVQWtmTzNVZGlUX2s2UEp0VXlXS2QyR1VaSWYza0F1dGRQdWtQYXdmajJNQk9JQUlrLiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInN1YiI6InktWmIySFF3dGQzRENXdGhNNXQyZzVfdTF6T1pQRWdWOTlvU0JrYnhTakUiLCJ0aWQiOiI1MWI3ZjM5MS05M2Q4LTRlZmUtOGYyNi1kNTMyNThhNzc2MTkiLCJ1bmlxdWVfbmFtZSI6ImxpdmUuY29tI2dveWFsLnlhc2hlbmRyYUBnbWFpbC5jb20iLCJ1dGkiOiJFbzI4MXNFa0JrQ3JLczJObmxvbUFBIiwidmVyIjoiMS4wIiwid2lkcyI6WyI2MmU5MDM5NC02OWY1LTQyMzctOTE5MC0wMTIxNzcxNDVlMTAiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX3RjZHQiOjE2NTEyOTg2MDd9.gvJAl9jDJTlu_ZGpB9VqUiDsqeoEQfu5uiTob8ug0nYxZYEmYbNGLXAwx1CA4katyQvgy6d9tO22Ou-SlUO0XjXNqh-92PUQ6zQjbBKcXfo8f-jI21MwZa6orYizFaAYsuziIwm63I2av155_QCXwr3ngQkLh8-avf0-U0q141m_ZHAkYOasp2uIvve2tY3zakGDW__EDw19ji99kDIXZTRZnnB894yvonT7GTBup04Fcz0iFtM82hDQyS79OOozubtycYVzsIbKvBi-SGoSwU8H7W3UhogEwMb16Gj3BNdbDl6q9uBopi9fyTnF9_2SRXffpq1jcTxfoRaCrJVRqw'
                } 
            }, 
            options: {
                encrypt: true, 
                database: 'cbs_changes',
                rowCollectionOnRequestCompletion: true
            } 
        };
        const connection = new Connection(config);
        connection.on('connect', err => {
            if (err) { 
                console.error('Failed to connect', err); 
                rej(err);
            } else { 
                console.log("Connected");
                res(connection);
            } 
        });
        connection.connect(); console.log('Done!!!');
    })
    
}

async function queryTable(connection, query) {
    return new Promise((res, rej) => {
        // const query = `SELECT
        //     TOP 1000 *
        //     FROM
        //         OPENROWSET(
        //             BULK 'https://ygazuredatalake.dfs.core.windows.net/ygdatalakesfile/changes/**',
        //             FORMAT = 'PARQUET'
        //         ) AS [result] WHERE result.[file] LIKE 'DEPTH' AND changeType = 2`
        let jsonData;
        var request = new Request(query, function(err, rowCount, rows) {
            // console.log(err, rowCount, rows)
            jsonData = rows?.map(row => {
                const json = {};
                row.forEach(col => {
                    json[col.metadata.colName] = col.value
                })
                return json;
            }) || [];
            // console.log(jsonData.length);
            console.log('Query Completed');
            // json
        });
        request.setTimeout(45000);
        connection.execSql(request);
        request.on('requestCompleted', () => {
            res(jsonData);
        })
    })
}

// main();

async function createTable() {
    const connection = await connect();
    const query = `CREATE TABLE [dbo].[changes_catalog]
    (
        [file] nvarchar(4000),
        [extension] nvarchar(4000),
        [fileType] int,
        [changeType] int,
        [inode] bigint,
        [size] bigint,
        [ctime] datetime2(7),
        [mtime] datetime2(7),
        [crtime] datetime2(7),
        [workingEnvironmentId] nvarchar(4000),
        [volumeId] nvarchar(4000),
        [snapshotId] nvarchar(4000)
    )
    `;


    var request = new Request(query, function(err, rowCount, rows) {
        console.log(err, rowCount, rows)
    });
    request.setTimeout(45000);
    connection.execSql(request);
}

async function createExternalTable() {
    const connection = await connect();
    const query = `CREATE EXTERNAL TABLE [dbo].[changes_catalog]
    (
        [file] nvarchar(4000),
        [extension] nvarchar(4000),
        [fileType] int,
        [changeType] int,
        [inode] bigint,
        [size] bigint,
        [ctime] datetime2(7),
        [mtime] datetime2(7),
        [crtime] datetime2(7),
        [workingEnvironmentId] nvarchar(4000),
        [volumeId] nvarchar(4000),
        [snapshotId] nvarchar(4000)
    )
    WITH
    (
        CLUSTERED COLUMNSTORE INDEX
    )
    `;


    var request = new Request(query, function(err, rowCount, rows) {
        console.log(err, rowCount, rows)
    });
    request.setTimeout(45000);
    connection.execSql(request);
}

// createExternalTable()
// createTable()

async function insertData() {
    const query = `INSERT INTO dbo.catalog VALUES ('Collin', 'Bashirian', 7, 4, '2022-05-18T15:40:33.774Z');`
    console.log(query);
    // const connection = await connect();

    // var request = new Request(query, function(err, rowCount, rows) {
    //     console.log(err, rowCount, rows)
    // });
    // request.setTimeout(45000);
    // connection.execSql(request);
}

async function createPartitionViews() {
    const connection = await connect();
    const query = `CREATE VIEW cbs_changes_prt_view
        AS SELECT *, results.filepath(1) AS [workingenvironmentid], results.filepath(2) AS [volumeid], results.filepath(3) AS [snapshotid]
        FROM
        OPENROWSET(
            BULK '/changes/workingenvironmentid=*/volumeid=*/snapshotid=*/**',
            DATA_SOURCE = 'ygdatalakesfile_ygazuredatalake_dfs_core_windows_net',
            FORMAT='PARQUET'
        ) AS results`;
  
    var request = new Request(query, function(err, rowCount, rows) {
        console.log(err, rowCount, rows)
    });
    request.setTimeout(45000);
    connection.execSql(request);
    request.on('requestCompleted', () => {
        connection.close()
    })
}

// createPartitionViews()

// insertData();
module.exports = {
    connect, queryTable, createTable
}