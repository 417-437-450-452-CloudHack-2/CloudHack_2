#importing of necessary libraries
import time
import pika
import json
import time
import os
# from sqlalchemy import create_engine
from pymongo import MongoClient

#sleeptime to handle system reboot
sleepTime = 20
time.sleep(sleepTime)

print(' [*] Connecting to server ...')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='ride-match-queue', durable=True) #declaration of queue for the Database

print(' [*] Waiting for messages.')

client = MongoClient('mongodb') #invokation of client for MongoDB
db = client['ride-db'] #naming of DB
collection = db["ride-col"] #creation of collection
print('[INFO] database CREATED.')

#callback function to handle insertion of data 
def callback(ch, method, properties, body):
    # create_db(db)
    # print(" [x] Received new ride data ")
    # json_obj = json.loads(body)
    # add_new_row(db,json_obj)
    # print(" [x] Done inserting")
    # get_row(db)
    # print(" [x] Done fetching")
    # print(' [*] Sleeping for ', sleep_time, ' seconds.')
    # time.sleep(sleep_time)
    # print(json_obj)
    json_obj = json.loads(body)
    collection.insert_one(json_obj)
    print("[INFO] Inserted ", json_obj, " into database")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
#channel.basic_consume(queue='task_queue', on_message_callback=callback)
channel.basic_consume(queue='ride-match-queue', on_message_callback=callback)
channel.start_consuming()