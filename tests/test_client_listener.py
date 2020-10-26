import json
import socket
import unittest
from multiprocessing import Process, Pipe, Barrier
from time import sleep

from backup_utils.blocking_socket_transferer import BlockingSocketTransferer
from src.client_listener.client_listener import ClientListener


class TestClientListener(unittest.TestCase):
    PORT = 5000

    def _launch_process(self, client_listener_send, client_listener_recv):
        self.backup_scheduler_send.close()
        try:
            client_listener = ClientListener(TestClientListener.PORT, 5, client_listener_send,
                                             client_listener_recv)
            self.barrier.wait()
            client_listener()
        except Exception as e:
            raise e

    def setUp(self):
        try:
            from pytest_cov.embed import cleanup_on_sigterm
        except ImportError:
            pass
        else:
            cleanup_on_sigterm()
        self.barrier = Barrier(2)
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
        TestClientListener.PORT += 1

    def test_send_and_receive_command(self):
        self.barrier.wait()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', TestClientListener.PORT))
        socket_transferer = BlockingSocketTransferer(sock)
        socket_transferer.send_plain_text('{"command": "dummy", "args": {"one": "one"}}')
        command, args = self.backup_scheduler_recv.recv()
        self.assertEqual(command, 'dummy')
        self.assertEqual(args, {"one": "one"})
        self.backup_scheduler_send.send(("OK", {}))
        msg = socket_transferer.receive_plain_text()
        self.assertEqual(json.loads(msg), {"message": "OK", "data": {}})
        socket_transferer.close()

    def test_error_json_and_then_working(self):
        self.barrier.wait()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', TestClientListener.PORT))
        socket_transferer = BlockingSocketTransferer(sock)
        socket_transferer.send_plain_text("asd")
        msg = socket_transferer.receive_plain_text()
        self.assertEqual(json.loads(msg)["message"], "ERROR")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', TestClientListener.PORT))
        socket_transferer = BlockingSocketTransferer(sock)
        socket_transferer.send_plain_text('{"command": "dummy", "args": {"one": "one"}}')
        command, args = self.backup_scheduler_recv.recv()
        self.assertEqual(command, 'dummy')
        self.assertEqual(args, {"one": "one"})
        self.backup_scheduler_send.send(("OK", {}))
        msg = socket_transferer.receive_plain_text()
        self.assertEqual(json.loads(msg), {"message": "OK", "data": {}})
        socket_transferer.close()

    def test_detect_backup_scheduler_dead(self):
        self.barrier.wait()
        self.backup_scheduler_send.close()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', TestClientListener.PORT))
        socket_transferer = BlockingSocketTransferer(sock)
        socket_transferer.send_plain_text('{"command": "dummy", "args": {"one": "one"}}')
        msg = socket_transferer.receive_plain_text()
        self.assertEqual("ABORT", msg)
        sleep(1)
        self.assertTrue(not self.p.is_alive())

    def test_detect_socket_dead(self):
        self.barrier.wait()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', TestClientListener.PORT))
        socket_transferer = BlockingSocketTransferer(sock)
        socket_transferer.send_plain_text('{"command": "dummy", "args": {"one": "one"}}')
        command, args = self.backup_scheduler_recv.recv()
        self.assertEqual(command, 'dummy')
        self.assertEqual(args, {"one": "one"})
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()
        self.backup_scheduler_send.send(("OK", {}))
        sleep(1)
        self.assertTrue(not self.p.is_alive())
