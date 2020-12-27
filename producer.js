
const fs = require('fs');
var host = process.argv[1];
var port = 5671;
var sent = 0, received=0;
var total = 500000
var obj = {"records": []}

var container = require('rhea');
container.options.username = "peerlro";
container.options.password = "peerlro123456789";
container.on('connection_open', function (context) {
    context.connection.open_sender('examples');
});

container.on('sendable', function (context) {
    while (sent < total) {
        context.sender.send({body: "{\"sendtime\": \"" + new Date().getTime().toString() + "\"}"});
        sent++;
    }
    context.sender.detach();
});
container.connect({port: port, host: host, transport: "ssl"});