#importing the necessary libraries
import pika
import json
import time
import os

#initial sleep to let the system boot up
sleepTime = 60
# print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

SERVER_IP = os.environ["SERVER_IP"] #getting server IP from docker compose
SERVER_PORT = os.environ["SERVER_PORT"] #getting server port from docker compose
CUST_ID= os.environ["CUST_ID"] #getting consumer ID from docker compose

print(' [*] Connecting to server ...')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True) #declaration of queue as per requirements 
# channel.queue_declare(queue='ride-match-queue', durable=True)

print(' [*] Waiting for messages.')

#callback function to handle consuming
def callback(ch, method, properties, body):
    print(" [x] Received new ride data ")
    json_obj = json.loads(body)
    sleep_time = int(json_obj['time']) #sleep time to show the working of customer booking a ride
    print(' [', CUST_ID , '] Sleeping for ', sleep_time, ' seconds.')
    time.sleep(sleep_time) #function which handles the sleep
    print(json_obj)
    ch.basic_ack(delivery_tag=method.delivery_tag) #acknowledgement

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback) 
# channel.basic_consume(queue='ride-match-queue', on_message_callback=callback2)
channel.start_consuming() #starts consumer function
 