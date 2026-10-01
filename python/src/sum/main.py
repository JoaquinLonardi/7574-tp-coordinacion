import os
import sys
import signal
import logging

from common import middleware, message_protocol, fruit_item


def stable_shard(fruit, shards):
    acc = 0
    for byte in fruit.encode():
        acc = acc * 31 + byte
    return acc % shards


ID = os.environ.get("ID")
MOM_HOST = os.environ.get("MOM_HOST")
INPUT_QUEUE = os.environ.get("INPUT_QUEUE")
SUM_PREFIX = os.environ.get("SUM_PREFIX")
SUM_CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
AGGREGATION_AMOUNT = os.environ.get("AGGREGATION_AMOUNT")
AGGREGATION_PREFIX = os.environ.get("AGGREGATION_PREFIX")

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.input_queue.listen_broadcast(
            SUM_CONTROL_EXCHANGE, f"{SUM_CONTROL_EXCHANGE}_{ID}"
        )
        self.data_output_exchanges = []
        for i in range(int(AGGREGATION_AMOUNT)):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit = {}

    def process_data(self, query_id, fruit, amount):
        fruits = self.amount_by_fruit.setdefault(query_id, {})
        fruits[fruit] = fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def flush(self, query_id):
        fruits = self.amount_by_fruit.pop(query_id, {})
        for final_fruit_item in fruits.values():
            shard = stable_shard(final_fruit_item.fruit, len(self.data_output_exchanges))
            self.data_output_exchanges[shard].send(
                message_protocol.internal.build_data(
                    query_id, final_fruit_item.fruit, final_fruit_item.amount
                )
            )
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.build_sum_eof(query_id, ID)
            )

    def process_message(self, message, ack, nack):
        message = message_protocol.internal.parse(message)
        if message["type"] == message_protocol.internal.DATA:
            self.process_data(message["query_id"], message["fruit"], message["amount"])
        elif message["type"] == message_protocol.internal.CLIENT_EOF:
            self.input_queue.send_broadcast(
                message_protocol.internal.build_sum_flush(message["query_id"])
            )
        elif message["type"] == message_protocol.internal.SUM_FLUSH:
            self.flush(message["query_id"])
        ack()

    def stop(self):
        self.input_queue.request_stop()

    def close(self):
        self.input_queue.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()

    def start(self):
        self.input_queue.start_consuming(self.process_message)
        self.close()

def main():
    logging.basicConfig(level=logging.INFO)
    for name, value in [
        ("ID", ID),
        ("MOM_HOST", MOM_HOST),
        ("INPUT_QUEUE", INPUT_QUEUE),
        ("SUM_PREFIX", SUM_PREFIX),
        ("AGGREGATION_AMOUNT", AGGREGATION_AMOUNT),
        ("AGGREGATION_PREFIX", AGGREGATION_PREFIX),
    ]:
        if value is None:
            logging.error(f"Falta la variable de entorno {name}")
            sys.exit(1)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: sum_filter.stop())
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
