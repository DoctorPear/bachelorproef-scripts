import pulsar
import threading, logging, time, sys, json, os

json_object_file = {"records": []}

url = sys.argv[0]

client = pulsar.Client('pulsar://' + url + ':6650')

msg_size = 1000
amount_of_messages = 40000000

producer_stop = threading.Event()
consumer_stop = threading.Event()

class Producer(threading.Thread):
    payload = b'1' * msg_size

    def run(self):
        producer = client.create_producer('my-topic')
        self.sent = 0

        while not producer_stop.is_set():
            if self.sent < amount_of_messages:
                msg = {"sendtime": int(round(time.time() * 1000)), "payload": self.payload}
                producer.send((json.dumps(msg)))
                self.sent += 1

class Consumer(threading.Thread):
    def run(self):
        consumer = client.subscribe('my-topic', 'my-subscription')
        going_on = True
        while going_on:
            if consumer_stop.is_set():
                client.close()
            msg = consumer.receive()
            result = json.loads(msg.data())
            adobj = {"sendtime": result['sendtime'], "receivetime": int(round(time.time() * 1000))}
            json_object_file["records"].append(adobj)
            consumer.acknowledge(msg)


def main():
    threads = [
        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(1)
    going_on = True
    while going_on:
        if threads[0].sent >= amount_of_messages:
            time.sleep(120)
            producer_stop.set()
            consumer_stop.set()
            going_on = False
    with open('pulsarResults.json', 'w') as outfile:
        json.dump(json_object_file, outfile)
    createEasyResults()
    os._exit(0)

def createEasyResults():
    total_latency = 0
    start_produce_time = json_object_file['records'][0]['sendtime']
    end_produce_time = json_object_file['records'][-1]['sendtime']
    start_receive_time = json_object_file['records'][0]['receivetime']
    end_receive_time = json_object_file['records'][-1]['receivetime']
    minimum_latency = 100000
    maximum_latency = 0
    for i in json_object_file['records']:
        latency = i['receivetime'] - i['sendtime']
        total_latency += latency
        if latency > maximum_latency:
            maximum_latency = latency
        if latency < minimum_latency:
            minimum_latency = latency
    json_result_obj = {}
    json_result_obj['average_latency'] = total_latency / amount_of_messages
    json_result_obj['minimum_latency'] = minimum_latency
    json_result_obj['maximum_latency'] = maximum_latency
    json_result_obj['total_send_time'] = (end_produce_time - start_produce_time) / 1000
    json_result_obj['total_receive_time'] = (end_receive_time - start_receive_time) / 1000
    json_result_obj['messages_per_second'] = amount_of_messages / (end_receive_time - start_receive_time) / 1000
    print(json_result_obj)
    with open('pulsarEasyResults.json', 'w') as outfile:
        json.dump(json_result_obj, outfile)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()