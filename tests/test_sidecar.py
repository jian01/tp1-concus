import json
import logging
import os
import random
import shutil
import socket
import unittest
from multiprocessing import Process
from time import sleep

from backup_server.src.backup_scheduler.node_handler_process import NodeHandlerProcess
from backup_utils.backup_file import BackupFile
from backup_utils.blocking_socket_transferer import BlockingSocketTransferer
from sidecar.src.sidecar_process import SidecarProcess


class TestSidecar(unittest.TestCase):
    PORT = random.randint(3000, 5000)

    def setUp(self) -> None:
        try:
            from pytest_cov.embed import cleanup_on_sigterm
        except ImportError:
            pass
        else:
            cleanup_on_sigterm()
        shutil.rmtree('/tmp/backup_output', ignore_errors=True)
        os.mkdir('/tmp/backup_output')
        with open('/tmp/example', 'w') as example_file:
            example_file.write("asd")
        BackupFile.create_from_path('/tmp/example', "/tmp/backup_output/out2")
        SidecarProcess.logger = logging.getLogger("dummy")
        self.sidecar_process = SidecarProcess(TestSidecar.PORT, 3)
        self.p = Process(target=self.sidecar_process)
        self.p.start()

    def tearDown(self) -> None:
        if self.p.is_alive():
            self.p.terminate()
        shutil.rmtree('/tmp/backup_output', ignore_errors=True)
        os.remove('/tmp/example')
        TestSidecar.PORT += 1

    def test_simple_backup(self):
        node_handler_process = NodeHandlerProcess('localhost', TestSidecar.PORT,
                                                  '/tmp/example',
                                                  '/tmp/backup_output/out',
                                                  'dummy_checksum')
        sleep(5)
        node_handler_process()
        expected_file = BackupFile.create_from_path('/tmp/example', "/tmp/backup_output/out2")
        backup_file = BackupFile("/tmp/backup_output/out")
        self.assertEqual(expected_file.get_hash(), backup_file.get_hash())

    def test_backup_same_checksum(self):
        expected_file = BackupFile.create_from_path('/tmp/example',
                                                    "/tmp/backup_output/out2")
        node_handler_process = NodeHandlerProcess('localhost', TestSidecar.PORT,
                                                  '/tmp/example',
                                                  '/tmp/backup_output/out',
                                                  expected_file.get_hash())
        sleep(5)
        node_handler_process()
        self.assertTrue(os.path.exists('/tmp/backup_output/out.SAME'))

    def test_node_handler_ends_when_unexistent_path(self):
        node_handler_process = NodeHandlerProcess('localhost', TestSidecar.PORT,
                                                  '/tmp/example2',
                                                  '/tmp/backup_output/out',
                                                  "dummy")
        sleep(5)
        node_handler_process()

    def test_fail_to_receive_file(self):
        sleep(5)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', TestSidecar.PORT))
        socket_transferer = BlockingSocketTransferer(sock)
        socket_transferer.send_plain_text(json.dumps({"checksum": "",
                                                      "path": '/tmp/example'}))
        _ = socket_transferer.receive_plain_text()
        socket_transferer.close()
        sleep(5)

    def test_fail_to_receive_order(self):
        sleep(5)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', TestSidecar.PORT))
        socket_transferer = BlockingSocketTransferer(sock)
        socket_transferer.close()
        sleep(5)
