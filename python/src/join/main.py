import os
import sys
import signal
import logging

from common import middleware, message_protocol, fruit_item, barrier


MOM_HOST = os.environ.get("MOM_HOST")
INPUT_QUEUE = os.environ.get("INPUT_QUEUE")
OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE")
AGGREGATION_AMOUNT = os.environ.get("AGGREGATION_AMOUNT")
TOP_SIZE = os.environ.get("TOP_SIZE")


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_tops = {}
        self.tops_barrier = barrier.Barrier(int(AGGREGATION_AMOUNT))

    def process_top(self, query_id, aggregation_id, top):
        if not self.tops_barrier.arrive(query_id, aggregation_id):
            return
        fruits = self.partial_tops.setdefault(query_id, [])
        for fruit, amount in top:
            fruits.append(fruit_item.FruitItem(fruit, amount))
        if not self.tops_barrier.is_complete(query_id):
            return
        self.tops_barrier.forget(query_id)
        fruits = self.partial_tops.pop(query_id)
        result = [
            (item.fruit, item.amount)
            for item in fruit_item.canonical_top(fruits, int(TOP_SIZE))
        ]
        self.output_queue.send(
            message_protocol.internal.build_result(query_id, result)
        )

    def process_message(self, message, ack, nack):
        message = message_protocol.internal.parse(message)
        self.process_top(
            message["query_id"], message["aggregation_id"], message["top"]
        )
        ack()

    def stop(self):
        self.input_queue.request_stop()

    def close(self):
        self.input_queue.close()
        self.output_queue.close()

    def start(self):
        self.input_queue.start_consuming(self.process_message)
        self.close()


def main():
    logging.basicConfig(level=logging.INFO)
    for name, value in [
        ("MOM_HOST", MOM_HOST),
        ("INPUT_QUEUE", INPUT_QUEUE),
        ("OUTPUT_QUEUE", OUTPUT_QUEUE),
        ("AGGREGATION_AMOUNT", AGGREGATION_AMOUNT),
        ("TOP_SIZE", TOP_SIZE),
    ]:
        if value is None:
            logging.error(f"Falta la variable de entorno {name}")
            sys.exit(1)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: join_filter.stop())
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
