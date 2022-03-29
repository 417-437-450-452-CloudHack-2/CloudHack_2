from flask import Flask, request
import pika
import json

app = Flask(__name__)

def add_new_ride(ride_details):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=ride_details,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    connection.close()
    return " [x] Sent to new_ride queue: %r" % ride_details

def send_to_db(ride_details):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='ride-match-queue', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='ride-match-queue',
        body=ride_details,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    connection.close()
    return " [x] Sent to db queue: %r" % ride_details
    


@app.route('/')
def index():
    return 'OK'


@app.route('/new-ride/', methods = ['POST'])
def new_ride():
    ride_details = json.dumps(request.get_json())
    add_new_ride(ride_details)
    send_to_db(ride_details)
    return " [x] Sent: new ride data to both queues"


@app.route('/new-ride-matching-consumer/', methods = ['POST'])
def new_ride_match():
    ride_match = json.dumps(request.get_json())
    name =ride_match["name"]
    consumer_id = ride_match["id"]
    addr=request.remote_addr
    arr.append(((name,addr),(consumer_id,addr))) #doubt, ask see in github
    print(arr)
    
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
