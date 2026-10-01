import json

# Mensajes del protocolo interno, tipados por el campo "type". JSON va bien en el
# canal interno; lo que el TP0 prohibe es JSON en el socket externo con el cliente.
DATA = "data"
CLIENT_EOF = "client_eof"
SUM_FLUSH = "sum_flush"
SUM_EOF = "sum_eof"
TOP = "top"
RESULT = "result"

TYPES = (DATA, CLIENT_EOF, SUM_FLUSH, SUM_EOF, TOP, RESULT)


def build_data(query_id, fruit, amount):
    return encode(DATA, query_id=query_id, fruit=fruit, amount=amount)


def build_client_eof(query_id):
    return encode(CLIENT_EOF, query_id=query_id)


def build_sum_flush(query_id):
    return encode(SUM_FLUSH, query_id=query_id)


def build_sum_eof(query_id, sum_id):
    return encode(SUM_EOF, query_id=query_id, sum_id=sum_id)


def build_top(query_id, aggregation_id, top):
    return encode(TOP, query_id=query_id, aggregation_id=aggregation_id, top=top)


def build_result(query_id, top):
    return encode(RESULT, query_id=query_id, top=top)


def parse(message):
    fields = json.loads(message.decode("utf-8"))
    if fields.get("type") not in TYPES:
        raise ValueError(f"Tipo de mensaje interno desconocido: {fields.get('type')}")
    return fields


def encode(message_type, **fields):
    fields["type"] = message_type
    return json.dumps(fields).encode("utf-8")
