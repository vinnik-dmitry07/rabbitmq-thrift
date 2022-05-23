import uuid
from threading import Thread

import pika
from thrift.TSerialization import serialize, deserialize

from calc.ttypes import InvalidOperation, Operation, Work, Result


def calculate(work):
    print(f'Calculate({work})')

    if work.op == Operation.ADD:
        val = work.num1 + work.num2
    elif work.op == Operation.SUBTRACT:
        val = work.num1 - work.num2
    elif work.op == Operation.MULTIPLY:
        val = work.num1 * work.num2
    elif work.op == Operation.DIVIDE:
        if work.num2 == 0:
            raise InvalidOperation(work.op, 'Cannot divide by 0')
        val = work.num1 / work.num2
    else:
        raise InvalidOperation(work.op, 'Invalid operation')

    return val


class RpcClient(object):
    def __init__(self):
        self.id1 = '1'  # to run on same device names of queues should differ
        self.id2 = '2'  # todo: remove

        self.response = None
        self.corr_id = None

        self.connection_client = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_client = self.connection_client.channel()
        self.callback_queue_client = self.channel_client.queue_declare(queue='client_queue' + self.id1).method.queue

        self.channel_client.basic_consume(
            queue=self.callback_queue_client,
            on_message_callback=self.on_response,
            auto_ack=True,
        )

        rpc_server = Thread(target=RpcClient.start_server, args=(self,))
        rpc_server.start()

    def start_server(self):
        connection_server = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel_server = connection_server.channel()
        channel_server.queue_declare(queue='server_queue' + self.id1)

        channel_server.basic_qos(prefetch_count=1)
        channel_server.basic_consume(queue='server_queue' + self.id1, on_message_callback=self.on_request)
        channel_server.start_consuming()

    # noinspection PyUnusedLocal
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    # noinspection PyMethodMayBeStatic
    def on_request(self, ch, method, props, body):
        work = deserialize(Work(), body)

        result = Result()
        try:
            result.val = calculate(work)
        except InvalidOperation as ouch:
            result.ouch = ouch

        ch.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=serialize(result)
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def call(self, work):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel_client.basic_publish(
            exchange='',
            routing_key='server_queue' + self.id2,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue_client,
                correlation_id=self.corr_id,
            ),
            body=serialize(work)  # binary by default
        )
        while self.response is None:
            self.connection_client.process_data_events()
        return deserialize(Result(), self.response)


if __name__ == '__main__':
    client = RpcClient()

    while True:
        expression = input('Enter expression: ')
        client_work = Work()

        symbol_operator = {
            '+': Operation.ADD,
            '-': Operation.SUBTRACT,
            '*': Operation.MULTIPLY,
            '/': Operation.DIVIDE,
        }

        has_operator = False
        for symbol, operator in symbol_operator.items():
            if symbol in expression:
                if has_operator:
                    raise ValueError(expression)

                split = expression.split(symbol)
                assert len(split) == 2, split

                client_work.num1 = int(split[0])
                client_work.op = operator
                client_work.num2 = int(split[1])

                has_operator = True
        if not has_operator:
            raise ValueError(expression)

        client_result = client.call(client_work)
        print(client_result.val or client_result.ouch)
