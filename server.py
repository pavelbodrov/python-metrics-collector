import asyncio
from exceptions import IncorrectCommandException


class Storage:
    def __init__(self):
        self._data = {}

    def put(self, metric, value, timestamp):
        if metric not in self._data:
            self._data[metric] = {}
        self._data[metric][timestamp] = value

    def get(self, metric):
        if metric == '*':
            return {metric: [(ts, val) for ts, val in values.items()] for metric, values in self._data.items()}
        else:
            return {metric: [(ts, val) for ts, val in self._data.get(metric, {}).items()]}


class RequestHandler:
    def __init__(self, storage):
        self.storage = storage

    def _parse_request(self, request):
        cmd = request.strip().split(' ')[0]
        args = request.strip().split(' ')[1:]
        if cmd == 'put':
            metric, value, timestamp = args
            self.storage.put(metric, float(value), int(timestamp))
            return ''
        elif cmd == 'get':
            metric = args[0]
            metrics_dict = self.storage.get(metric)
            if metrics_dict:
                records = []
                for metric, values in metrics_dict.items():
                    if values:
                        for ts, val in values:
                            records.append(f'{metric} {val} {ts}')
                return '\n'.join(records) + '\n' if records else ''
        else:
            raise IncorrectCommandException

    def process_data(self, data):
        status = 'ok'
        response = ''

        for request in data.split('\n'):
            if request:
                try:
                    response += self._parse_request(request)
                except IncorrectCommandException:
                    status = 'error'
                    response = 'wrong command'
                except Exception:
                    status = 'error'
                    response = ''
        return f'{status}\n{response}\n'


class ClientServerProtocol(asyncio.Protocol):
    storage = Storage()

    def __init__(self):
        super().__init__()
        self.request_handler = RequestHandler(storage=self.storage)

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        print(repr(data.decode()))
        response = self.request_handler.process_data(data.decode())
        print(repr(response))
        self.transport.write(response.encode())


def run_server(host, port):
    loop = asyncio.get_event_loop()

    server = loop.run_until_complete(
        loop.create_server(ClientServerProtocol, host, port)
    )

    try:
        print(f'Server is running at {host}:{port}')
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == '__main__':
    run_server('127.0.0.1', 8181)
