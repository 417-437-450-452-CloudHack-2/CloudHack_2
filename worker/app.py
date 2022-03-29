import pika
import json
import time

sleepTime = 20
# print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

print(' [*] Connecting to server ...')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)
# channel.queue_declare(queue='ride-match-queue', durable=True)

print(' [*] Waiting for messages.')


def callback(ch, method, properties, body):
    print(" [x] Received new ride data ")
    json_obj = json.loads(body)
    sleep_time = int(json_obj['time'])
    print(' [*] Sleeping for ', sleep_time, ' seconds.')
    time.sleep(sleep_time)
    print(json_obj)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# def callback2(ch, method, properties, body):
#     print(" [x] Received ride match data ")
#     json_str = body
#     print(json_str)
#     ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)
# channel.basic_consume(queue='ride-match-queue', on_message_callback=callback2)
channel.start_consuming()
