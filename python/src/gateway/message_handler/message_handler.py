import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.query_id = str(uuid.uuid4())

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.build_data(self.query_id, fruit, amount)

    def serialize_eof_message(self, message):
        return message_protocol.internal.build_client_eof(self.query_id)

    def deserialize_result_message(self, message):
        result = message_protocol.internal.parse(message)
        if result["query_id"] != self.query_id:
            return []
        return result["top"]
