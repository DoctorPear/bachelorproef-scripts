
const fs = require('fs');
var host = process.argv[1];
var port = 5671;
var sent = 0, received=0;
var total = 40000000
var obj = {"records": []}

var container = require('rhea');
container.options.username = "peerlro";
container.options.password = "peerlro123456789";
container.on('connection_open', function (context) {
    context.connection.open_receiver('examples');
});
container.on('message', function (context) {
    console.log(context.message.body);
    var o = JSON.parse(context.message.body);
    o.receivetime = new Date().getTime().toString();
    obj.records.push(o);
    received++;
    if (received == total) {
        createResults();
        context.connection.close();
    }
});

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
    var minimum_latency = 100000;
    var maximum_latency = 0;
    for (const i of obj.records) {
        var latency = i.receivetime - i.sendtime;
        total_latency += latency;
        if (latency > maximum_latency) {
            maximum_latency = latency;
        }
        if (latency < minimum_latency) {
            minimum_latency = latency;
        }
    }
    var result = {};
    result.messages_per_second = total / ((end_consume_time - start_consume_time) / 1000);
    result.average_latency = total_latency / total;
    result.minimum_latency = minimum_latency;
    result.maximum_latency = maximum_latency;
    result.total_send_time = (end_produce_time - start_produce_time) / 1000;
    result.total_receive_time = (end_consume_time - start_consume_time) / 1000;
    fs.writeFile('./activemqEasyResults.json', JSON.stringify(result), 'utf8', (err) => {
        if (!err) {
            console.log('done');
        }
    });
}

container.connect({port: port, host: host, transport: "ssl"});