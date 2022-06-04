const { AccessControlClient } = require("@azure/synapse-access-control");
const { DefaultAzureCredential, logger } = require("@azure/identity");
const { SynapseManagementClient } = require('@azure/arm-synapse');
const { StorageManagementClient } = require('@azure/arm-storage');
const { BlobServiceClient } = require("@azure/storage-blob");
const { ArtifactsClient } = require('@azure/synapse-artifacts');
const { connect, queryTable } = require("./synapse_connect");
const axios = require('axios').default;

const subscriptionId = "cc2cfdc5-1119-482f-b822-cf0edd1c041b";
const resourceGroupName = "my_azure_rg";

const workspaceName = 'yg-synapse-workspace';

const token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImpTMVhvMU9XRGpfNTJ2YndHTmd2UU8yVnpNYyIsImtpZCI6ImpTMVhvMU9XRGpfNTJ2YndHTmd2UU8yVnpNYyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldC8iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC81MWI3ZjM5MS05M2Q4LTRlZmUtOGYyNi1kNTMyNThhNzc2MTkvIiwiaWF0IjoxNjU0MTU4NzgxLCJuYmYiOjE2NTQxNTg3ODEsImV4cCI6MTY1NDE2MzA5MSwiYWNyIjoiMSIsImFpbyI6IkFXUUFtLzhUQUFBQWU3NXhMVTM4d0Z3ck1JbDlIbmx5NGQ2cnJxUzJXdmFSUEMxeGNNWEVuaUdBS21wWC8wQWlsaDJiZE9IUGJTMGdSRGtCelBTd0tsdXFWQllDVXpUTjdMb1kxQXVBcGR4bWxLTzg1UTVHSlg5ZWVhL1dKbXBoZ29NcTViQXYyQjg4IiwiYWx0c2VjaWQiOiIxOmxpdmUuY29tOjAwMDM3RkZFMEQ3OUFDNkUiLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiMDRiMDc3OTUtOGRkYi00NjFhLWJiZWUtMDJmOWUxYmY3YjQ2IiwiYXBwaWRhY3IiOiIwIiwiZW1haWwiOiJnb3lhbC55YXNoZW5kcmFAZ21haWwuY29tIiwiZmFtaWx5X25hbWUiOiJHb3lhbCIsImdpdmVuX25hbWUiOiJZYXNoZW5kcmEiLCJncm91cHMiOlsiMmEzYjZhOWQtMzY5Mi00OTIxLWIwYmEtMzI3MWI1N2Q5MWE0Il0sImlkcCI6ImxpdmUuY29tIiwiaXBhZGRyIjoiNDkuMjA3LjIxMy4yMjQiLCJuYW1lIjoiWWFzaGVuZHJhIEdveWFsIiwib2lkIjoiMzRjYjE1ZjktNzkzMC00MjE4LThlNGMtNDZiZWNhOTI2NzMwIiwicHVpZCI6IjEwMDMyMDAxRjU2NDA2MTgiLCJyaCI6IjAuQVZVQWtmTzNVZGlUX2s2UEp0VXlXS2QyR1VaSWYza0F1dGRQdWtQYXdmajJNQk9JQUlrLiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInN1YiI6InktWmIySFF3dGQzRENXdGhNNXQyZzVfdTF6T1pQRWdWOTlvU0JrYnhTakUiLCJ0aWQiOiI1MWI3ZjM5MS05M2Q4LTRlZmUtOGYyNi1kNTMyNThhNzc2MTkiLCJ1bmlxdWVfbmFtZSI6ImxpdmUuY29tI2dveWFsLnlhc2hlbmRyYUBnbWFpbC5jb20iLCJ1dGkiOiJqaktyeEJXU3QwLW51TnRLYndJWkFRIiwidmVyIjoiMS4wIiwid2lkcyI6WyI2MmU5MDM5NC02OWY1LTQyMzctOTE5MC0wMTIxNzcxNDVlMTAiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX3RjZHQiOjE2NTEyOTg2MDd9.AlYUw-tLzf3gwCEOaM6cJ7LHfabwlBdmn0dsEoBK_pfQbGDCBcdYmWWqJ2gNyiSVZL5dCMbsW8SySGyyLqnNrru8DMiI7G-MGz43w4d9rDxXIM6zPOK7QVmdjQEgaeNBl_InKkL4T4aMR8wdEQa0o6GMTWSSPuz3iZPfbCczavVWPXyA7lvZkYGWoFtmkDh4KL3bXbVMT4BrsC8SIUgNv4eIpg0YDyhjGqXKoRPd0_-KfvVghMYRl8p0DdZ3zmlxXcZK4ztdokgvr20_jGtSVoVcifD2PN6Fl2zZiL9vndKy3MX0cM_EBhkeynu05x79ZbxWCw1zdKkcFjTKmWPqyQ';

const url = `https://management.azure.com/subscriptions/${subscriptionId}/resourceGroups/${resourceGroupName}/providers/Microsoft.Synapse/workspaces/${workspaceName}?api-version=2021-06-01`

// const url = `https://management.azure.com/subscriptions/cc2cfdc5-1119-482f-b822-cf0edd1c041b/resourceGroups/%7BresourceGroupName%7D/providers/Microsoft.Synapse/workspaces/%7BworkspaceName%7D?api-version=2021-06-01`

// async function createWorkspace() {
//     try {
//         const response = await axios({
//             url,
//             method: 'PUT',
//             data: {
//                 location: 'East US',
//                 identity: {
//                     type: "SystemAssigned"
//                 },
//                 properties: {
//                     defaultDataLakeStorage: {
//                     accountUrl: "https://ygazuredatalakenew.dfs.core.windows.net",
//                     filesystem: "default"
//                     }
//                 }
//             },
//             headers: {
//                 Authorization: `Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImpTMVhvMU9XRGpfNTJ2YndHTmd2UU8yVnpNYyIsImtpZCI6ImpTMVhvMU9XRGpfNTJ2YndHTmd2UU8yVnpNYyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldCIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzUxYjdmMzkxLTkzZDgtNGVmZS04ZjI2LWQ1MzI1OGE3NzYxOS8iLCJpYXQiOjE2NTE3NDMxMjcsIm5iZiI6MTY1MTc0MzEyNywiZXhwIjoxNjUxNzQ3NjIzLCJhY3IiOiIxIiwiYWlvIjoiQVdRQW0vOFRBQUFBWTBrK2pNM0FxS0lBRi9iYlZuOFVjR2lkVGVBVFo2YkpJSVVCTDd5RFJhVENZdXBDTzVHNmdvbkVrVDZzRXV3L0pBVDhNMkxFaEloWTZQRy93MjhjZ2VKL3g1cjBOZEpCMUdOWlpuOUs3eFgwdXljUlBiWWxkdlg3WDVoZ01uTTIiLCJhbHRzZWNpZCI6IjE6bGl2ZS5jb206MDAwMzdGRkUwRDc5QUM2RSIsImFtciI6WyJwd2QiLCJtZmEiXSwiYXBwaWQiOiIxOGZiY2ExNi0yMjI0LTQ1ZjYtODViMC1mN2JmMmIzOWIzZjMiLCJhcHBpZGFjciI6IjAiLCJlbWFpbCI6ImdveWFsLnlhc2hlbmRyYUBnbWFpbC5jb20iLCJmYW1pbHlfbmFtZSI6IkdveWFsIiwiZ2l2ZW5fbmFtZSI6Illhc2hlbmRyYSIsImdyb3VwcyI6WyIyYTNiNmE5ZC0zNjkyLTQ5MjEtYjBiYS0zMjcxYjU3ZDkxYTQiXSwiaWRwIjoibGl2ZS5jb20iLCJpcGFkZHIiOiIyMDIuMy4xMjEuNCIsIm5hbWUiOiJZYXNoZW5kcmEgR295YWwiLCJvaWQiOiIzNGNiMTVmOS03OTMwLTQyMTgtOGU0Yy00NmJlY2E5MjY3MzAiLCJwdWlkIjoiMTAwMzIwMDFGNTY0MDYxOCIsInJoIjoiMC5BVlVBa2ZPM1VkaVRfazZQSnRVeVdLZDJHVVpJZjNrQXV0ZFB1a1Bhd2ZqMk1CT0lBSWsuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoieS1aYjJIUXd0ZDNEQ1d0aE01dDJnNV91MXpPWlBFZ1Y5OW9TQmtieFNqRSIsInRpZCI6IjUxYjdmMzkxLTkzZDgtNGVmZS04ZjI2LWQ1MzI1OGE3NzYxOSIsInVuaXF1ZV9uYW1lIjoibGl2ZS5jb20jZ295YWwueWFzaGVuZHJhQGdtYWlsLmNvbSIsInV0aSI6InVGMkNReDMtYmstcmFEclF2ZE54QUEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbIjYyZTkwMzk0LTY5ZjUtNDIzNy05MTkwLTAxMjE3NzE0NWUxMCIsImI3OWZiZjRkLTNlZjktNDY4OS04MTQzLTc2YjE5NGU4NTUwOSJdLCJ4bXNfdGNkdCI6MTY1MTI5ODYwN30.J8wFe3v1Kg7Sg2G8FU0l9hHiK9Ar2OavdWdUXJHGJH1K-Ak3ayls2f6YbFAoejE0-0-44AklR6irjQS797tMI1ZCiSm1213v8gU7KBkaNqiSTDVSrsj7_IF0w6M8fksD6v_iQ7Irolp4LJ0zQQIjDRv0uUL_lnC6WhyIX2v4FEiQwf1KO9L3hooXjQYQNfUE9M4H8jU_UjMc2VcosTGSX-JiGkb8Ms-pNd-UxiYMY05zORTTuGjx6fEbp8DsOJVZ9uO2y7Rov6TfdgPtEDDJWB3eyb4nyCx3GV3gIEJOSuI0dKtrLWowo252AUJMEcwLCjbV9kcbTSrVbJGf8P-9Iw`,
//                 "Content-type": "application/json"
//             }
//         });
//         console.log(response);
//       } catch (error) {
//         console.error(error);
//       }
// }


async function getCredentials() {
    // const { token } = await axios.get(`${CLOUD_MANAGER_CREDENTIALS_SERVER_ADDRESS}/credentials/azure/subscription/${subscription}`);

    return {
        getToken: () => ({
            token, 
            expiresOnTimestamp: Date.now() + 60 * 60 * 1000 
        })
    };
}


async function getSynapseManagementClient() {
    const credentials = await getCredentials();

    return new SynapseManagementClient(credentials, subscriptionId);
}

async function getStorageManagementClient() {
    const credentials = await getCredentials();

    return new StorageManagementClient(credentials, subscriptionId);
}

async function getSynapseWorkspaces() {
    const client = await getSynapseManagementClient();

    const response = [];
    for await (const page of client.workspaces.list().byPage()) {
        for (const keyProperties of page) {
            response.push(keyProperties);
        }
    }

    console.log('Check workspaces ', response);
}

async function createStorageAccount() {
    const client = await getStorageManagementClient();
    const accName = 'ygazurepocsa'
    await client.storageAccounts.beginCreateAndWait(resourceGroupName, accName, {
        kind: 'BlockBlobStorage',
        location: 'eastus',
        sku: {
            name: 'Premium_LRS',
            tier: 'Premium'
        },
        accessTier: 'Cool',
        identity: {
            type: 'SystemAssigned'
        },
        isHnsEnabled: true,
        allowBlobPublicAccess: true
    });
    console.log('Storage Account Created!!');

    // const { keys } = await client.storageAccounts.listKeys(resourceGroupName, accName);
    // const blobClient = new BlobServiceClient()
}


async function createSynapseWorkspace() {
    const client = await getSynapseManagementClient();
    await client.workspaces.beginCreateOrUpdateAndWait(resourceGroupName, 'yg-azure-poc-ws', {
        location: 'eastus',
        identity: {
            type: 'SystemAssigned'
        },
        defaultDataLakeStorage: {
            accountUrl: 'https://ygazurepocsa.dfs.core.windows.net',
            filesystem: 'default'
        }
    })
    console.log('Done');
    client.workspaces
}


async function main() {
    // const cred = await getCredentials();
    // const client = await getSynapseManagementClient();
    // const ws = await client.workspaces.get(resourceGroupName, 'yg-azure-poc-ws');
    // console.log(ws);
    // const artClient = new ArtifactsClient(cred, 'yg-azure-poc-ws', 'https://yg-azure-poc-ws.dev.azuresynapse.net');

    // artClient.workspaceOperations

    // IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	// CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	// WITH ( FORMAT_TYPE = PARQUET)

    // IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'default_ygazurepocsa_dfs_core_windows_net') 
	// CREATE EXTERNAL DATA SOURCE [default_ygazurepocsa_dfs_core_windows_net] 
	// WITH (
	// 	LOCATION = 'abfss://default@ygazurepocsa.dfs.core.windows.net' 
	// )

    const connection = await connect();
    // const query = `CREATE EXTERNAL TABLE cbs_catalog (
    //     [name] nvarchar(4000),
    //     [companyName] nvarchar(4000),
    //     [experience] bigint,
    //     [salary] bigint,
    //     [date] datetime2(7)
    //     )
    //     WITH (
    //     LOCATION = 'employees-Conroy.parquet',
    //     DATA_SOURCE = [default_ygazurepocsa_dfs_core_windows_net],
    //     FILE_FORMAT = [SynapseParquetFormat]
    // )`
    // const dbName = 'yg_catalog_poc';
    // const query = `CREATE DATABASE ${dbName}`;

    const query = `SELECT TOP 100 * FROM dbo.cbs_catalog`;
    // const query = `SELECT
    //     TOP 100 *
    //     FROM
    //     OPENROWSET(
    //         BULK 'https://ygazurepocsa.dfs.core.windows.net/default/employees-Conroy.parquet',
    //         FORMAT = 'PARQUET'
    //     ) AS [result]`
    const data = await queryTable(connection, query);
    console.log(data);
}

main()
// createStorageAccount();
// createSynapseWorkspace();
// getSynapseWorkspaces();

