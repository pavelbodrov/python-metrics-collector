import socket
import time
from collections import defaultdict
from exceptions import ClientError

class Client:
    def __init__(self, host, port, timeout=None):
        self.conn = socket.create_connection((host, port), timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def put(self, metric, value, timestamp=None):
        timestamp = timestamp or int(time.time())
        try:
            self.conn.sendall(f'put {metric} {value} {timestamp}\n'.encode())
            res = self.conn.recv(1024).decode('utf8').split('\n')
        except Exception as e:
            raise ClientError(str(e))

    def _parse_response(self, res):
        result_dict = defaultdict(list)
        for record in res:
            if record != '':
                metric, value, ts = record.split(' ')
                result_dict[metric].append((int(ts), float(value)))
                result_dict[metric].sort(key=lambda x: x[0])
        return result_dict
    
    def get(self, metric):
        try:
            self.conn.sendall(f'get {metric}\n'.encode())
            res = self.conn.recv(1024).decode()
            status, *data = res.split('\n')

            if status == 'ok':
                return self._parse_response(data)
            else:
                raise ClientError
        except Exception as e:
            raise ClientError(str(e))

if __name__ == '__main__':
    with Client('127.0.0.1', 8181) as client:
        client.put('palm.cpu', .3, timestamp=1150864248)
        time.sleep(0.3)
        client.put('palm.ram', 36)
        client.put('palm.cpu', 0.7)
        client.put('oak.cpu', 0.9)
        print(client.get('*'))