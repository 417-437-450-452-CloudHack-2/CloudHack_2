import time
import random
import pika
import json
import time
from sqlalchemy import create_engine

sleepTime = 20
# print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

print(' [*] Connecting to server ...')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
#channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue='ride-match-queue', durable=True)

print(' [*] Waiting for messages.')

db_name = 'database'
db_user = 'username'
db_pass = 'secret'
db_host = 'db'
db_port = '5432'

# Connecto to the database
db_string = 'postgres://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)

def add_new_row(json_obj):
    # Insert a new number into the 'numbers' table.
    db.execute("INSERT INTO ride_details (pickup,destination,sleep_time,cost,seats) "+\
        "VALUES ("+\ 
        json_obj['pickup'] + "," + \
        json_obj['destination']  + "," + \
        int(json_obj['time'])  + "," + \
        json_obj['cost']  + "," + \
        json_obj['seats']  + ");")

def get_row():
    # Retrieve the last number inserted inside the 'numbers'
    query = "SELECT * from ride_details LIMIT 5;"

    result_set = db.execute(query)  
    for (r) in result_set:  
        return r[0]

def callback(ch, method, properties, body):
    print(" [x] Received new ride data ")
    json_obj = json.loads(body)
    add_new_row(json_obj)
    print(" [x] Done inserting")
    get_row()
    print(" [x] Done fetching")
    #print(' [*] Sleeping for ', sleep_time, ' seconds.')
    #time.sleep(sleep_time)
    #print(json_obj)
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