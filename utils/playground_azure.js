// const { AccessControlClient } = require("@azure/synapse-access-control");
// const { DefaultAzureCredential, logger } = require("@azure/identity");
// const { SynapseManagementClient } = require('@azure/arm-synapse');
// const { StorageManagementClient } = require("@azure/storage-blob");
// const { ArtifactsClient } = require('@azure/synapse-artifacts');
// const axios = require('axios').default;

// const subscriptionId = "cc2cfdc5-1119-482f-b822-cf0edd1c041b";
// const resourceGroupName = "my_azure_rg";

// const workspaceName = 'yg-synapse-workspace';

// const token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImpTMVhvMU9XRGpfNTJ2YndHTmd2UU8yVnpNYyIsImtpZCI6ImpTMVhvMU9XRGpfNTJ2YndHTmd2UU8yVnpNYyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldC8iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC81MWI3ZjM5MS05M2Q4LTRlZmUtOGYyNi1kNTMyNThhNzc2MTkvIiwiaWF0IjoxNjUyMTU5ODY2LCJuYmYiOjE2NTIxNTk4NjYsImV4cCI6MTY1MjE2NTIzOSwiYWNyIjoiMSIsImFpbyI6IkFXUUFtLzhUQUFBQXNOcHMrRWczMXR6R2ExZzdvbk5WbXFZaXQrQTBDTGpjZ3U5WnJzZmt5MnJWOW9hYkJRTmdFaitmeGJ1SkVEa1lKYzBKK3lUOVN5ZkRiYlZmVmt3dEVCWFBuQm1vc0hPUkNwTk5vcWRkazkzWEQzOHUxc3dZYUplallKcHVnWWFIIiwiYWx0c2VjaWQiOiIxOmxpdmUuY29tOjAwMDM3RkZFMEQ3OUFDNkUiLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiMDRiMDc3OTUtOGRkYi00NjFhLWJiZWUtMDJmOWUxYmY3YjQ2IiwiYXBwaWRhY3IiOiIwIiwiZW1haWwiOiJnb3lhbC55YXNoZW5kcmFAZ21haWwuY29tIiwiZmFtaWx5X25hbWUiOiJHb3lhbCIsImdpdmVuX25hbWUiOiJZYXNoZW5kcmEiLCJncm91cHMiOlsiMmEzYjZhOWQtMzY5Mi00OTIxLWIwYmEtMzI3MWI1N2Q5MWE0Il0sImlkcCI6ImxpdmUuY29tIiwiaXBhZGRyIjoiNDkuMjA3LjIwMC4yMjMiLCJuYW1lIjoiWWFzaGVuZHJhIEdveWFsIiwib2lkIjoiMzRjYjE1ZjktNzkzMC00MjE4LThlNGMtNDZiZWNhOTI2NzMwIiwicHVpZCI6IjEwMDMyMDAxRjU2NDA2MTgiLCJyaCI6IjAuQVZVQWtmTzNVZGlUX2s2UEp0VXlXS2QyR1VaSWYza0F1dGRQdWtQYXdmajJNQk9JQUlrLiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInN1YiI6InktWmIySFF3dGQzRENXdGhNNXQyZzVfdTF6T1pQRWdWOTlvU0JrYnhTakUiLCJ0aWQiOiI1MWI3ZjM5MS05M2Q4LTRlZmUtOGYyNi1kNTMyNThhNzc2MTkiLCJ1bmlxdWVfbmFtZSI6ImxpdmUuY29tI2dveWFsLnlhc2hlbmRyYUBnbWFpbC5jb20iLCJ1dGkiOiJtS0l6SFBlS19FcVpyd01GX2hWNkFBIiwidmVyIjoiMS4wIiwid2lkcyI6WyI2MmU5MDM5NC02OWY1LTQyMzctOTE5MC0wMTIxNzcxNDVlMTAiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX3RjZHQiOjE2NTEyOTg2MDd9.RAfe4tKasWvFWLmZydKI7Qt0g-w3Jm96BS6xZnEustVT9QbgKV5uEZqfpXIHg_CJjhvJdnWX3IZyHVIVtpsZ1ZxEtsfbpTpBW8cwpMrjEMgY0lAsiA0cNdWuIDsyQ8fUcmJ6IqDaZM5tuEpAEyxcT8ty8qkSbBb3aAyq2uGPxl9Ns9d3-kQ1ZpeVqxaFgD1LSYGSqjEb6Ara0uYSXTnst7OSz7CB4xmUaIsgyQcoTj2qKai4bXLfl8y6QUsyv-pBA6odWmbmTYWePdM2P2CQ-ZixwGU5d1zuQ31E-IZx6_RBgVgQVWxVy61AASN3pLJf8eUWPggmQUF0slv_lQ9tXQ';

// const url = `https://management.azure.com/subscriptions/${subscriptionId}/resourceGroups/${resourceGroupName}/providers/Microsoft.Synapse/workspaces/${workspaceName}?api-version=2021-06-01`

// // const url = `https://management.azure.com/subscriptions/cc2cfdc5-1119-482f-b822-cf0edd1c041b/resourceGroups/%7BresourceGroupName%7D/providers/Microsoft.Synapse/workspaces/%7BworkspaceName%7D?api-version=2021-06-01`

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

// // main()


// async function getCredentials() {
//     // const { token } = await axios.get(`${CLOUD_MANAGER_CREDENTIALS_SERVER_ADDRESS}/credentials/azure/subscription/${subscription}`);

//     return {
//         getToken: () => ({
//             token, 
//             expiresOnTimestamp: Date.now() + 60 * 60 * 1000 
//         })
//     };
// }


// async function getSynapseManagementClient() {
//     const credentials = await getCredentials();

//     return new SynapseManagementClient(credentials, subscriptionId);
// }

// async function getSynapseWorkspaces() {
//     const client = await getSynapseManagementClient();

//     // const response = [];
//     // for await (const page of client.workspaces.list().byPage()) {
//     //     for (const keyProperties of page) {
//     //         response.push(keyProperties);
//     //     }
//     // }

//     // console.log('Check workspaces ', response);
    

//     // const response = [];
//     // for await (const page of client.sqlPools.listByWorkspace(resourceGroupName, workspaceName).byPage()) {
//     //     for (const keyProperties of page) {
//     //         response.push(keyProperties);
//     //     }
//     // }

//     // console.log(response);

//     const sqlPool = await client.sqlPools.get(resourceGroupName, workspaceName, 'catalogsqlpool');
//     sqlPoo
// }

// getSynapseWorkspaces();

