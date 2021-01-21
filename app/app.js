const express = require('express');
const bodyParser = require('body-parser');
cors = require('cors');

const { request } = require('express');

const app = express();

// Middleware
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(cors());

// Routes
app.get('/', (req,resp) => {
    resp.send('bouyakaaaaa');
});

console.log('Application is running on port 8888!');

app.listen(8888);