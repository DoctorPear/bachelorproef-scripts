import threading, logging, time, sys
import pika
import ssl
import functools
import json
import os

json_object_file = {"records": []}

url = sys.argv[1]
creds = pika.PlainCredentials("peerlro", "peerlro123456789")
context = ssl.create_default_context()
ssl_options = pika.SSLOptions(context, url)
params = pika.ConnectionParameters(credentials=creds, host=url, ssl_options=ssl_options)

msg_size = 1000
amount_of_messages = 500000

producer_stop = threading.Event()
consumer_stop = threading.Event()

def on_message(chan, method_frame, _header_frame, body, userdata=None):
    result = json.loads(body)
    adobj = {"sendtime": result['sendtime'], "receivetime": int(round(time.time() * 1000))}
    json_object_file["records"].append(adobj)
    chan.basic_ack(delivery_tag=method_frame.delivery_tag)

class Producer(threading.Thread):
    payload = b'1' * msg_size

    def run(self):
        prodconn = pika.BlockingConnection(params)
        prodchann = prodconn.channel()
        prodchann.exchange_declare(exchange='test_exchange', exchange_type='direct', passive=False, durable=True, auto_delete=False)
        self.sent = 0

        while not producer_stop.is_set():
            if self.sent < amount_of_messages:
                msg = {"sendtime": int(round(time.time() * 1000)), "payload": self.payload}
                prodchann.basic_publish(exchange='test_exchange', routing_key='standard_key', body=json.dumps(msg), properties=pika.BasicProperties(content_type='application/json'))
                self.sent += 1
        prodchann.close()

class Consumer(threading.Thread):
    def run(self):
        conconn = pika.BlockingConnection(params)
        channel = conconn.channel()
        channel.exchange_declare(
            exchange='test_exchange',
            exchange_type='direct',
            passive=False,
            durable=True,
            auto_delete=False)
        channel.queue_declare(queue='standard', auto_delete=True)
        channel.queue_bind(
            queue='standard', exchange='test_exchange', routing_key='standard_key')
        channel.basic_qos(prefetch_count=1000)
        on_message_callback = functools.partial(
            on_message, userdata='on_message_userdata')
        channel.basic_consume('standard', on_message_callback)

        channel.start_consuming()
        going_on = True
        while going_on:
            if consumer_stop.is_set():
                channel.stop_consuming()
                going_on = False
                channel.close()

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
            print('Messages sent: %d' % threads[0].sent)
            going_on = False
    with open('rabbitmqResults.json', 'w') as outfile:
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
    with open('rabbitmqEasyResults.json', 'w') as outfile:
        json.dump(json_result_obj, outfile)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()