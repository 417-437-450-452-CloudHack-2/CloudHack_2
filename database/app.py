import time
import pika
import json
import time
import os
# from sqlalchemy import create_engine
from pymongo import MongoClient

sleepTime = 20
# print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

print(' [*] Connecting to server ...')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
#channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue='ride-match-queue', durable=True)

print(' [*] Waiting for messages.')

# db_name = os.getenv['POSTGRES_DB']
# db_user = os.getenv['POSTGRES_USER']
# db_pass = os.getenv['POSTGRES_PASSWORD']
# db_host = 'localhost'
# db_port = '5432'

# # Connecto to the database
# db_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
# db = create_engine(db_string)

# def create_db(db):
#     # Create the database
#     db.execute("CREATE TABLE IF NOT EXISTS ride_details (pickup varchar(255), destination varchar(255), sleep_time integer, cost float, seats integer;")

# def add_new_row(db,json_obj):
#     # Insert a new number into the 'numbers' table.
#     db.execute("INSERT INTO ride_details (pickup,destination,sleep_time,cost,seats) "+\
#         "VALUES (" + \
#         json_obj['pickup'] + "," + \
#         json_obj['destination']  + "," + \
#         int(json_obj['time'])  + "," + \
#         json_obj['cost']  + "," + \
#         json_obj['seats']  + ");")

# def get_row(db):
#     # Retrieve the last number inserted inside the 'numbers'
#     query = "SELECT * from ride_details LIMIT 5;"

#     result_set = db.execute(query)  
#     for (r) in result_set:  
#         return r[0]

client = MongoClient('mongodb')
db = client['ride-db']
collection = db["ride-col"]
print('[INFO] database CREATED.')

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


'''def database_consume():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='database', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')


    def callback(ch, method, properties, body):
        #Put body into database
        arr=body.decode('utf-8').split()
        print(" [x] Received %r" % body)
        mydict = { "pickup": arr[0], "destination": arr[1],"time": arr[2],"seats": arr[3],"cost": arr[4] }
        x = mycol.insert_one(mydict)
        # time.sleep(body.count(b'.'))
        print("asdasdasd")
        print(" [x] Done", x)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='database', on_message_callback=callback)

    channel.start_consuming()

database_consume()
'''