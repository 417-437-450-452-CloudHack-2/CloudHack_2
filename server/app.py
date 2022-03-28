from flask import Flask, request
import pika
import json

app = Flask(__name__)


@app.route('/')
def index():
    return 'OK'


@app.route('/new-ride/', methods = ['POST'])
def add_new_ride():
    ride_details = json.dumps(request.get_json())
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
    return " [x] Sent: new ride data"

@app.route('/new-ride-matching-consumer/', methods = ['POST'])
def new_ride_match():
    ride_match = json.dumps(request.get_json())
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='ride-match-queue', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='ride-match-queue',
        body=ride_match,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    connection.close()
    return " [x] Sent: ride match data"


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
