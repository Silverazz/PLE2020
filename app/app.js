const express = require('express');
const bodyParser = require('body-parser');
const { request } = require('express');

const app = express();


// Moteur de template
app.set('view engine', 'ejs');

// Middleware
app.use('/assests', express.static('public'));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

// Routes
app.get('/', (req,resp) => {
    resp.render('index');
});

console.log('Application is running on port 8888!');

app.listen(8888);