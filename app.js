const express = require('express');
const { connect, queryTable } = require('./utils/synapse_connect');
const app = express();

async function main() {
    app.set('view engine', 'ejs');
    app.set('views', './views');
    app.use('/static', express.static('public'));

    app.get('/', function(req, res){
        res.render('index');
    })

    app.get('/search', async function(req, res) {
        const connection = await connect();
        console.log(req.query);
        let query = `SELECT TOP (1000) [file]
        ,[extension]
        ,[fileType]
        ,[changeType]
        ,[size]
        ,[crtime]
        ,[workingenvironmentid]
        ,[volumeid]
        ,[snapshotid]
        FROM [dbo].[changes_view]
        `;


        if (Object.keys(req.query).length) {
            query += ' WHERE ';
        }

        
        let condAdded = false;
        for(const key in req.query) {
            const val = req.query[key];
            if (condAdded) {
                query += ' AND ';
            }
            switch (key) {
                case 'file':
                    query += `[file] LIKE '%${val}%'`;
                    break;
                case 'fileType':
                case 'changeType':
                case 'size':
                    query += `[${key}] = ${val}`
                    break;
                case 'extension':
                case 'workingenvironmentid':
                    query += `[${key}] = '${val}'`
                    break;
                default:
                    query += `[${key}] = ${val}`
            }
            condAdded = true;
        }
        
        console.log(query);

        const data = await queryTable(connection, query);

        res.send({ data })
    })

    app.listen(3000, () => console.log('Server started on port 3000'));
}

main()