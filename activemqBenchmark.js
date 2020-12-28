
const fs = require('fs');
var host = "b-fff7c675-7d60-4cb8-830f-7ecab3f0f885-1.mq.eu-west-1.amazonaws.com";
var port = 5671;
var sent = 0, received=0;
var total = 2048;
var obj = {"records": []}

var container = require('rhea');
container.options.username = "peerlro";
container.options.password = "peerlro123456789";
container.on('connection_open', function (context) {
    context.connection.open_receiver('examples');
    context.connection.open_sender('examples');
});
container.on('message', function (context) {
    console.log(context.message.body);
    var old = JSON.parse(context.message.body);
    var o = {};
    o.sendtime = old.sendtime;
    o.receivetime = new Date().getTime().toString();
    obj.records.push(o);
    received++;
    if (received == total) {
        createResults();
        context.connection.close();
    }
});
container.on('sendable', function (context) {
    var payload = random(1000);
    while (sent < total) {
        context.sender.send({body: "{\"sendtime\": \"" + new Date().getTime().toString() + "\", \"payload\": \"" + payload + "\"}"});
        sent++;
    }
    context.sender.detach();
});
container.connect({port: port, host: host, transport: "ssl"});

function createResults() {
    fs.writeFile('./activemqResults.json', JSON.stringify(obj), 'utf8', (err) => {
        if (!err) {
            console.log('done');
        }
    });
    var start_produce_time = obj.records[0].sendtime;
    var end_produce_time = obj.records[obj.records.length - 1].sendtime;
    var start_consume_time = obj.records[0].receivetime;
    var end_consume_time = obj.records[obj.records.length - 1].receivetime;
    var total_latency = 0;
    for (const i of obj.records) {
        total_latency += (i.receivetime - i.sendtime) / 1000;
    }
    var result = {};
    result.messages_per_second = total / ((end_consume_time - start_consume_time) / 1000);
    result.average_latency = total_latency / total;
    result.total_send_time = (end_produce_time - start_produce_time) / 1000;
    result.total_receive_time = (end_consume_time - start_consume_time) / 1000;
    fs.writeFile('./activemqEasyResults.json', JSON.stringify(result), 'utf8', (err) => {
        if (!err) {
            console.log('done');
        }
    });
}

const random = (length = 8) => {
    // Declare all characters
    let chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

    // Pick characers randomly
    let str = '';
    for (let i = 0; i < length; i++) {
        str += chars.charAt(Math.floor(Math.random() * chars.length));
    }

    return str;

};