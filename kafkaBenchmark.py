#!/usr/bin/env python
from __future__ import print_function
import threading, logging, time, json, sys, os

from kafka import KafkaConsumer, KafkaProducer

msg_size = 1000
amount_of_messages = 500000

producer_stop = threading.Event()
consumer_stop = threading.Event()

class Producer(threading.Thread):
    big_msg = b'1' * msg_size

    def run(self):
        producer = KafkaProducer(bootstrap_servers=sys.argv[1]+':9092', batch_size=1000, value_serializer=lambda x: dumps(x).encode('utf-8'))
        self.sent = 0

        while not producer_stop.is_set():
            if self.sent < amount_of_messages:
                msg = {"sendtime": int(round(time.time() * 1000)), "payload": self.big_msg}
                producer.send('my-topic', json.dumps(msg))
                self.sent += 1
        producer.flush()


class Consumer(threading.Thread):

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=sys.argv[1]+':9092', auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group', value_serializer=lambda x: loads(x.decode('utf-8')))
        consumer.subscribe(['my-topic'])

        for message in consumer:
            result = json.loads(message)
            adobj = {"sendtime": result['sendtime'], "receivetime": int(round(time.time() * 1000))}
            json_object_file["records"].append(adobj)

            if consumer_stop.is_set():
                break

        consumer.close()

def main():
    threads = [
        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(20)
    going_on = True
    while going_on:
        if threads[0].sent >= amount_of_messages:
            time.sleep(120)
            producer_stop.set()
            consumer_stop.set()
            print('Messages sent: %d' % threads[0].sent)
            going_on = False
    with open('kafkaResults.json', 'w') as outfile:
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
    with open('kafkaEasyResults.json', 'w') as outfile:
        json.dump(json_result_obj, outfile)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()