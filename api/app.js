const express = require('express');
const bodyParser = require('body-parser');
const hbase = require('hbase');

cors = require('cors');

const { request } = require('express');

const app = express();

// Middleware
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(cors());

let client = new hbase.Client();

// client.status_cluster( function( error, statusCluster ){
//     console.info( statusCluster )
//   } );

// client
//     .table('al-jda-database')
//     .schema((err, sch) => {
//         console.info(err)
//     })

    // client.tables((error, tables) => {
    //     console.info(tables)
    //   })   

// client
//     .table("al-jda-database")
//     .row("pt")
//     .get("nbTweetLang:total", (err,[cell]) => {
//         console.log(cell)
//         console.log(JSON.stringify(cell.$));
//     });

// Routes
app.get('/user/:username', (req,resp) => {
    //return user information (nb tweet + hashtags)
    resp.send('ok');
});

app.get('/test', (req,resp) => {
    //return user information (nb tweet + hashtags)
    resp.send('test ok');
});

app.get('/top-hashtag/:k', (req,resp) => {
    //return user information (nb tweet + hashtags)
});


console.log('Application is running on port 9090!');

app.listen(9090);