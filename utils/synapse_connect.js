const { Connection, Request } = require('tedious');
async function connect() { 
    console.log('Starting...');
    return new Promise((res, rej) => {
        const config = { 
            server: 'yg-azure-poc-ws-ondemand.sql.azuresynapse.net',
            authentication: { 
                // type: 'default',
                type: 'azure-active-directory-default',
                options: { 
                    // token: ''
                    // userName: 'sqladminuser',
                    // password: 'Netapp123'
                }
            }, 
            options: {
                encrypt: true, 
                database: 'yg_catalog_poc',
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
            console.log({ query, err, rowCount, rows });
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
            connection.close();
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
/**
 *  We might need to create external data source
    IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'azuredatalakefiles_azuredatalakecatalog_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [azuredatalakefiles_azuredatalakecatalog_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://azuredatalakefiles@azuredatalakecatalog.dfs.core.windows.net' 
	)
 */

    
    // CREATE VIEW cbs_changes_azuredatalakecatalog_sa
    //     AS SELECT *, results.filepath(1) AS [workingenvironmentid], results.filepath(2) AS [volumeid], results.filepath(3) AS [snapshotid]
    //     FROM
    //     OPENROWSET(
    //         BULK '/changes/workingenvironmentid=*/volumeid=*/snapshotid=*/**',
    //         DATA_SOURCE = 'azuredatalakefiles_azuredatalakecatalog_dfs_core_windows_net',
    //         FORMAT='PARQUET'
    //     )
    //     WITH (
    //         [file] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS,
    //         extension varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS,
    //         fileType smallint,
    //         changeType smallint,
    //         inode bigint,
    //         size bigint,
    //         ctime datetime2,
    //         mtime datetime2,
    //         crtime datetime2
    //     )
    //     AS results

    //--------------- USE THIS COLLATE SQL_Latin1_General_CP1_CI_AS instead of Latin1_General_100_BIN2_UTF8 -------------


    const connection = await connect();
    const query = `CREATE VIEW cbs_changes_ygazuredatalake_sa_ci
        AS SELECT *,
        CAST(results.filepath(1) AS VARCHAR(40)) AS [workingenvironmentid], 
        CAST(results.filepath(2) AS VARCHAR(40)) AS [volumeid], 
        CAST(results.filepath(3) AS VARCHAR(40)) AS [snapshotid]
        FROM
        OPENROWSET(
            BULK '/changes/workingenvironmentid=*/volumeid=*/snapshotid=*/**',
            DATA_SOURCE = 'ygdatalakesfile_ygazuredatalake_dfs_core_windows_net',
            FORMAT='PARQUET'
        )
        WITH (
            [file] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS,
            extension varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS,
            fileType bigint,
            changeType bigint,
            inode bigint,
            size bigint,
            ctime datetime2,
            mtime datetime2,
            crtime datetime2
        )
        AS results`;
  
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
    connect, queryTable, createTable, createPartitionViews
}