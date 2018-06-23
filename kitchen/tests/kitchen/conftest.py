import logging
import os
import pytest
import requests
import socket
import subprocess
import tenacity

def _find_free_port(hostname, start_port=8000, attempts=1000):
    for p in range(start_port, start_port+attempts):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            with sock:
                sock.bind((hostname, p))
                return sock.getsockname()[1]
        except:
            pass  # try the next one
    else:
        raise Exception('Could not find a free port for the Kitchen server.')


class KitchenServer():
    def __init__(self):
        self.kitchen_path = os.getenv('KITCHEN_PATH', './bin/kitchen')
        self.hostname = os.getenv('KITCHEN_HOSTNAME', 'localhost')
        port_string = os.getenv('KITCHEN_PORT')
        self.port = int(port_string) if port_string else _find_free_port(self.hostname)
        args = [self.kitchen_path, '--hostname', self.hostname, '--port', str(self.port)]
        self.__server_process = subprocess.Popen(args)
        self.await_server()

    def await_server(self, max_wait_seconds=60):
        @tenacity.retry(stop=tenacity.stop_after_delay(max_wait_seconds), wait=tenacity.wait_fixed(1))
        def await_helper():
            assert requests.get(self.url())
        await_helper()
        logging.info(f'Kitchen server has been started on {self.hostname}:{self.port}')

    def url(self, path='/', scheme='http'):
        assert path.startswith('/')
        return f'{scheme}://{self.hostname}:{self.port}{path}'

    def kill(self):
        self.__server_process.terminate()
        logging.info(f'Kitchen server has been killed')

@pytest.fixture(scope="session", autouse=True)
def kitchen_server(request):
    """Manages an instance of the Kitchen test app server."""
    server = KitchenServer()
    request.addfinalizer(server.kill)
    return server
