import socket
import unittest
from multiprocessing import Barrier, Process

from backup_utils.blocking_socket_transferer import BlockingSocketTransferer


def process_sender(barrier, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', port))
    sock.listen(4)
    barrier.wait()
    c, addr = sock.accept()
    transferer = BlockingSocketTransferer(c)
    transferer.send_plain_text("Hola uacho")
    c.close()


class TestBlockingSocketTransferer(unittest.TestCase):
    TEST_PORT = 9000

    def setUp(self) -> None:
        self.barrier = Barrier(2)
        self.p = Process(target=process_sender, args=(self.barrier, TestBlockingSocketTransferer.TEST_PORT))
        self.p.start()

    def tearDown(self) -> None:
        self.p.terminate()

    def test_send_text(self):
        self.barrier.wait()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', TestBlockingSocketTransferer.TEST_PORT))
        socket_transferer = BlockingSocketTransferer(sock)
        self.assertEqual(socket_transferer.receive_plain_text(), "Hola uacho")
