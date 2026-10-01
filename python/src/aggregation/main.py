import os
import sys
import signal
import logging

from common import middleware, message_protocol, fruit_item, barrier


ID = os.environ.get("ID")
MOM_HOST = os.environ.get("MOM_HOST")
OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE")
SUM_AMOUNT = os.environ.get("SUM_AMOUNT")
AGGREGATION_PREFIX = os.environ.get("AGGREGATION_PREFIX")
TOP_SIZE = os.environ.get("TOP_SIZE")


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_tops = {}
        self.sum_barrier = barrier.Barrier(int(SUM_AMOUNT))

    def process_data(self, query_id, fruit, amount):
        fruits = self.fruit_tops.setdefault(query_id, {})
        fruits[fruit] = fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, amount)

    def process_eof(self, query_id, sum_id):
        self.sum_barrier.arrive(query_id, sum_id)
        if not self.sum_barrier.is_complete(query_id):
            return
        self.sum_barrier.forget(query_id)
        fruits = self.fruit_tops.pop(query_id, {})
        top = [
            (item.fruit, item.amount)
            for item in fruit_item.canonical_top(fruits.values(), int(TOP_SIZE))
        ]
        self.output_queue.send(
            message_protocol.internal.build_top(query_id, ID, top)
        )

    def process_message(self, message, ack, nack):
        message = message_protocol.internal.parse(message)
        if message["type"] == message_protocol.internal.DATA:
            self.process_data(message["query_id"], message["fruit"], message["amount"])
        elif message["type"] == message_protocol.internal.SUM_EOF:
            self.process_eof(message["query_id"], message["sum_id"])
        ack()

    def stop(self):
        self.input_exchange.request_stop()

    def close(self):
        self.input_exchange.close()
        self.output_queue.close()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)
        self.close()


def main():
    logging.basicConfig(level=logging.INFO)
    for name, value in [
        ("ID", ID),
        ("MOM_HOST", MOM_HOST),
        ("OUTPUT_QUEUE", OUTPUT_QUEUE),
        ("SUM_AMOUNT", SUM_AMOUNT),
        ("AGGREGATION_PREFIX", AGGREGATION_PREFIX),
        ("TOP_SIZE", TOP_SIZE),
    ]:
        if value is None:
            logging.error(f"Falta la variable de entorno {name}")
            sys.exit(1)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: aggregation_filter.stop())
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
