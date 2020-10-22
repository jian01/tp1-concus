import json
import socket
import unittest
from multiprocessing import Process, Pipe
from time import sleep

from src.client_listener.client_listener import ClientListener


class TestClientListener(unittest.TestCase):
    def _launch_process(self, client_listener_send, client_listener_recv):
        try:
            client_listener = ClientListener(1234, 5, client_listener_send,
                                             client_listener_recv)
            client_listener()
        except Exception as e:
            print('cagada')

    def setUp(self):
        try:
            from pytest_cov.embed import cleanup_on_sigterm
        except ImportError:
            pass
        else:
            cleanup_on_sigterm()
        self.client_listener = None
        self.backup_scheduler_recv, client_listener_send = Pipe(False)
        client_listener_recv, self.backup_scheduler_send = Pipe(False)
        self.p = Process(target=self._launch_process,
                         args=(client_listener_send, client_listener_recv))
        self.p.start()

    def tearDown(self) -> None:
        if self.p.is_alive():
            self.p.terminate()
        self.backup_scheduler_send.close()
        self.backup_scheduler_recv.close()

    def test_send_and_receive_command(self):
        sleep(3)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 1234))
        sock.sendall('{"command": "dummy", "args": {"one": "one"}}'.encode('utf-8'))
        command, args = self.backup_scheduler_recv.recv()
        self.assertEqual(command, 'dummy')
        self.assertEqual(args, {"one": "one"})
        self.backup_scheduler_send.send(("OK", {}))
        msg = sock.recv(2048).rstrip()
        self.assertEqual(json.loads(msg), {"message": "OK", "data": {}})
        sock.close()
