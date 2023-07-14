import json
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost', heartbeat=600, blocked_connection_timeout=300))
channel = connection.channel()


def publish(method, body):
    properties = pika.BasicProperties(method)
    channel.basic_publish(exchange='', routing_key='likes', body=json.dumps(body), properties=properties)
    print(channel.is_closed)


import pika
import time
from .models import Message


class Producer:
    def __init__(self, host, default_queue=None):
        self.host = host
        self.default_queue = default_queue

    def publish(self, message_content, queue=None):
        if queue is None:
            if self.default_queue is None:
                raise Exception("No queue specified and no default queue set.")
            else:
                queue = self.default_queue
        if self.rabbitmq_server_is_online():
            return self.send_to_rabbitmq(message_content, queue)
        else:
            return self.save_to_local_queue(message_content, queue)

    def rabbitmq_server_is_online(self):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            connection.close()
            return True
        except pika.exceptions.AMQPConnectionError:
            return False

    def send_to_rabbitmq(self, message_content, queue):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            channel = connection.channel()
            channel.queue_declare(queue=queue)
            channel.basic_publish(exchange='', routing_key=queue, body=message_content)
            connection.close()
            return True
        except Exception as e:
            print(f"An error occurred while sending to RabbitMQ: {e}")
            return False

    def save_to_local_queue(self, message_content, queue):
        try:
            Message.objects.create(content=message_content, queue=queue)
            return True
        except Exception as e:
            print(f"An error occurred while saving to local queue: {e}")
            return False

    def when_server_comes_online(self, queue=None):
        if queue is None:
            if self.default_queue is None:
                raise Exception("No queue specified and no default queue set.")
            else:
                queue = self.default_queue
        messages = self.get_all_messages_from_local_queue(queue)
        for message in messages:
            if self.publish(message.content, queue):
                message.delete()

    def get_all_messages_from_local_queue(self, queue):
        return Message.objects.filter(queue=queue).order_by('created_at')

    def start_heartbeat(self, interval, queue=None):
        while True:
            if self.rabbitmq_server_is_online():
                self.when_server_comes_online(queue)
            time.sleep(interval)
