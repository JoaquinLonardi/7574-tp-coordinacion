import pika

from .middleware import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
    MessageMiddlewareCloseError,
)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.publish_channel = self.connection.channel()
        self.publish_channel.queue_declare(queue=queue_name, durable=True)
        self.broadcast_exchange = None
        self.broadcast_queue = None
        self.current_channel = None

    def listen_broadcast(self, exchange_name, queue_name):
        self.broadcast_exchange = exchange_name
        self.broadcast_queue = queue_name
        self.publish_channel.exchange_declare(exchange_name, exchange_type="fanout")
        self.publish_channel.queue_declare(queue=queue_name, durable=True)
        self.publish_channel.queue_bind(exchange=exchange_name, queue=queue_name)

    def send_broadcast(self, message):
        self.publish_channel.basic_publish(self.broadcast_exchange, "", message)

    def start_consuming(self, on_message_callback):
        channel = self.connection.channel()
        self.current_channel = channel
        # De a uno: reparte parejo entre replicas y evita que el EOF se adelante al dato.
        channel.basic_qos(prefetch_count=1)

        def callback_wrapper(ch, method, props, body):
            # on message callback es así:
            # message - El valor tal y como lo recibe el método send de esta clase.
            # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
            # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            return on_message_callback(body, ack, nack)

        channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback_wrapper
        )
        if self.broadcast_queue:
            channel.basic_consume(
                queue=self.broadcast_queue, on_message_callback=callback_wrapper
            )

        channel.start_consuming()

    def stop_consuming(self):
        if self.current_channel:
            self.current_channel.stop_consuming()
            self.current_channel = None

    def request_stop(self):
        if self.connection.is_open:
            self.connection.add_callback_threadsafe(self.stop_consuming)

    def send(self, message):
        self.publish_channel.basic_publish("", self.queue_name, message)

    def close(self):
        try:
            if self.current_channel and self.current_channel.is_open:
                self.current_channel.close()
            if self.publish_channel.is_open:
                self.publish_channel.close()
            if self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.routing_keys = routing_keys
        self.exchange_name = exchange_name
        self.publish_channel = self.connection.channel()
        self.publish_channel.exchange_declare(exchange_name, exchange_type="direct")
        # Cola nombrada y durable, declarada y bindeada al construir (no al consumir),
        # para que exista desde el arranque y no se pierda lo que el productor ya publico.
        self.queue_name = f"{exchange_name}.{'.'.join(routing_keys)}"
        self.publish_channel.queue_declare(queue=self.queue_name, durable=True)
        for routing_key in routing_keys:
            self.publish_channel.queue_bind(
                exchange=exchange_name, queue=self.queue_name, routing_key=routing_key
            )
        self.current_channel = None

    def start_consuming(self, on_message_callback):
        channel = self.connection.channel()
        self.current_channel = channel
        channel.basic_qos(prefetch_count=1)

        def callback_wrapper(ch, method, props, body):
            # on message callback es así:
            # message - El valor tal y como lo recibe el método send de esta clase.
            # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
            # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            return on_message_callback(body, ack, nack)

        channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback_wrapper
        )

        channel.start_consuming()

    def stop_consuming(self):
        if self.current_channel:
            self.current_channel.stop_consuming()
            self.current_channel = None

    def request_stop(self):
        if self.connection.is_open:
            self.connection.add_callback_threadsafe(self.stop_consuming)

    def send(self, message):
        for routing_key in self.routing_keys:
            self.publish_channel.basic_publish(self.exchange_name, routing_key, message)

    def close(self):
        try:
            if self.current_channel and self.current_channel.is_open:
                self.current_channel.close()
            if self.publish_channel.is_open:
                self.publish_channel.close()
            if self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))
