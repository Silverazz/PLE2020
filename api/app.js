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


app.get('/languages', (req,res) => {
    client
        .table('al-jda-lang')
        .row('*')
        .get("total:value", (err,cell) => 
            err ? res.sendStatus(404) : res.json(cell)
        );
});

app.get('/user-nb-tweet/:username', (req,res) => {
    client
        .table('al-jda-user-tweet')
        .row(req.params.username)
        .get("total:value", (err,cell) => 
            err ? res.sendStatus(404) : res.json(cell)
        );
});

app.get('/user-tweets/:username', (req,res) => {
    client
        .table('al-jda-user-hashtag-list')
        .row(req.params.username)
        .get("hashtag:list", (err,cell) => 
            err ? res.sendStatus(404) : res.json(cell)
        );
});

app.get('/is-user-fake/:username', (req,res) => {
    client
        .table('al-jda-fake-influencer')
        .row(req.params.username)
        .exists("hashtag:list", (err,exists) => {
            if(err)
                res.sendStatus(404);
            else
                exists ? res.sendStatus(204) : res.sendStatus(404)
        });
});

app.get('/fake-users/:start/:batch', (req, res) => {
    const scanner = client
        .table('al-jda-fake-influencer')
        .scan({
            startRow: req.params.start,
            maxVersions: 1
        })

    const rows = []

    scanner.on('readable', () => {
        let max = 0
        let chunk

        while ((chunk = scanner.read()) && max < req.params.batch) {
            rows.push(chunk)
            max++
        }
        scanner.emit('end', null);
    })

    scanner.on('error', err =>
        res.sendStatus(404)
    )

    scanner.on('end', () =>
        res.json(rows)
    )
});

app.get('/users/:start/:batch', (req, res) => {
    const scanner = client
        .table('al-jda-user-tweet')
        .scan({
            startRow: req.params.start,
            maxVersions: 1
        })

    const rows = []

    scanner.on('readable', () => {
        let max = 0
        let chunk

        while ((chunk = scanner.read()) && max < req.params.batch) {
            rows.push(chunk)
            max++
        }
        scanner.emit('end', null);
    })

    scanner.on('error', err =>
        res.sendStatus(404)
    )

    scanner.on('end', () =>
        res.json(rows)
    )
});



console.log('Application is running on port 9090!');

app.listen(9090);



// console.log(scanner);


// ali ali

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
    // .get("nbTweetLang:total", (err,[cell]) => {
    //     console.log(cell)
    //     console.log(JSON.stringify(cell.$));
    // });

// Routes
// app.get('/user/:username', (req,res) => {
//     let table = client
//     .table('al-jda-top-hashtag')
//     .row('\x00\x00\x00\x00\x00\x00\x00\x13')
//     .get("hashtag", (err,[cell]) => {
//         if(err)
//             console.log(err);
//         else
//         // console.log(JSON.stringify(cell.$));
//         console.info(cell);
//     });
//     res.send('ok');
// });

// client
//     .table('al-jda-lang')
//     .row('*')
//     .get("total", (err,cell) => {
//         if(err)
//             console.log(err);
//         else
//         // console.log(JSON.stringify(cell.$));
//         console.info(cell);
//     });
    // client
    // .table('al-jda-lang')
    // .row('en')
    // .get("hashtag:number", (err,[cell]) => {
    //     console.log('bouyaaa '+(00000001<<32));
    //     if(err)
    //         console.log(err);
    //     else
    //     // console.log(JSON.stringify(cell.$));
    //     console.info(cell);
    // });

    // app.get('/top-hashtag/:k', (req,res) => {
//     //return user information (nb tweet + hashtags)
// });

// const myScanner = new hbase.Scanner(client, {table: 'al-jda-lang'})

// myScanner.

// const scanner = client
//     .table('al-jda-user-tweet')
//     .scan({
//         startRow: String.fromCharCode(0),
//         // endRow: String.fromCharCode(30),
//         maxVersions: 1,
//         batch: 10000
//     })

// const rows = []



// scanner.on('readable', () => {
//     let chunk
//     while(chunk = scanner.read()){
//         rows.push(chunk)
//     }
// })

// scanner.on('error', err =>
//     console.log(err)
// )

// scanner.on('end',() =>
//     console.info(rows.length)
// )


// app.get('/user/:username', (req,res) => {
//     let username = req.params.username
//     let data = []
//     client
//         .table('al-jda-user-tweet')
//         .row(username)
//         .get("total:value", (err,cell) => {
//             // if(err)
//             //     // res.sendStatus(404);
//             // else
//                 data.push(cell)
//         });
//     client
//         .table('al-jda-user-hashtag-list')
//         .row(username)
//         .get("hashtag", (err,cell) => {
//             // if(err)
//             //     // res.sendStatus(404);
//             // else
//             data.push(cell)
//         });
//     client
//         .table('al-jda-fake-influencer')
//         .row(username)
//         .exists("total", (err,exists) => {
//             // if(err)
//             //     // res.sendStatus(404);
//             // else
//             data.push(exists)
//         });
//     res.json(data)
// });