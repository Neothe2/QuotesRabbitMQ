import json
import pika
import time
from .models import Message


class Producer:
    def __init__(self, host, default_queue=None, default_method=None):
        self.host = host
        self.default_queue = default_queue
        self.default_method = default_method

    def publish(self, message_content, queue=None, method=None):
        print('publish method was called')
        message_json = json.dumps(message_content)

        if queue is None:
            if self.default_queue is None:
                raise Exception("No queue specified and no default queue set.")
            else:
                queue = self.default_queue

        if method is None:
            if self.default_method is None:
                raise Exception("No method specified and no default method set.")
            else:
                method = self.default_method

        if self.rabbitmq_server_is_online():
            print(f"RabbitMQ server is online. Publishing message to queue '{queue}'.")
            return self.send_to_rabbitmq(message_json, queue, method)
        else:
            print(f"RabbitMQ server is offline. Saving message to local queue '{queue}'.")
            return self.save_to_local_queue(message_json, queue, method)

    def rabbitmq_server_is_online(self):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            connection.close()
            print(f"RabbitMQ server at '{self.host}' is online.")
            return True
        except pika.exceptions.AMQPConnectionError:
            print(f"Failed to connect to RabbitMQ server at '{self.host}'.")
            return False

    def send_to_rabbitmq(self, message_content, queue, method):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            channel = connection.channel()
            channel.queue_declare(queue=queue)
            properties = pika.BasicProperties(method)
            channel.basic_publish(exchange='', routing_key=queue, body=message_content, properties=properties)
            connection.close()
            print(f"Successfully sent {message_content} to RabbitMQ queue '{queue}' with method '{method}'.")
            return True
        except Exception as e:
            print(f"An error occurred while sending to RabbitMQ: {e}")
            return False

    def save_to_local_queue(self, message_content, queue, method):
        try:
            Message.objects.create(content=message_content, queue=queue, method=method)
            print(f"Successfully saved message to local queue '{queue}'.")
            return True
        except Exception as e:
            print(f"An error occurred while saving to local queue: {e}")
            return False

    def when_server_comes_online(self, queue=None):
        # if queue is None:
        #     if self.default_queue is None:
        #         raise Exception("No queue specified and no default queue set.")
        #     else:
        #         queue = self.default_queue
        #
        # if method is None:
        #     if self.default_method is None:
        #         raise Exception("No method specified and no default method set.")
        #     else:
        #         method = self.default_method

        print(f"RabbitMQ server is online. Sending all messages from local queue '{queue}' to RabbitMQ.")
        messages = self.get_all_messages_from_local_queue(queue)
        for message in messages:
            if message.method is None or '':
                raise Exception('The message does not have a method associated with it')
            if self.publish(json.loads(message.content), message.queue, message.method):
                message.delete()

    def get_all_messages_from_local_queue(self, queue):
        return Message.objects.filter(queue=queue).order_by('created_at')

    def start_heartbeat(self, interval, queue=None):
        while True:
            print("checked")
            if self.rabbitmq_server_is_online():
                print("server is online")
                self.when_server_comes_online(queue)
            else:
                print("server is offline")
            time.sleep(interval)
