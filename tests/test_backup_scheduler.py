import os
import shutil
import unittest
from multiprocessing import Process, Pipe, Barrier

from src.backup_scheduler import node_handler_process
from src.database.disk_database import DiskDatabase


class MockNodeHandler:
    BARRIER = None

    def __init__(self, node_address: str, node_port: int,
                 node_path: str, write_file_path: str):
        self.node_address = node_address
        self.node_port = node_port
        self.node_path = node_path
        self.write_file_path = write_file_path

    def __call__(self, *args, **kwargs):
        open(self.write_file_path, "w").close()
        open(self.write_file_path + ".CORRECT", "w").close()
        MockNodeHandler.BARRIER.wait(timeout=10)


class TestBackupScheduler(unittest.TestCase):
    def setUp(self):
        try:
            from pytest_cov.embed import cleanup_on_sigterm
        except ImportError:
            pass
        else:
            cleanup_on_sigterm()
        self.barrier = Barrier(2)
        MockNodeHandler.BARRIER = self.barrier
        self.node_handler = node_handler_process.NodeHandlerProcess
        node_handler_process.NodeHandlerProcess = MockNodeHandler
        from src.backup_scheduler.backup_scheduler import BackupScheduler
        shutil.rmtree('/tmp/disk_db_concus', ignore_errors=True)
        os.mkdir('/tmp/disk_db_concus')
        database = DiskDatabase('/tmp/disk_db_concus')
        shutil.rmtree('/tmp/backup_scheduler_path', ignore_errors=True)
        os.mkdir('/tmp/backup_scheduler_path')
        backup_scheduler_recv, self.client_listener_send = Pipe(False)
        self.client_listener_recv, backup_scheduler_send = Pipe(False)
        backup_scheduler = BackupScheduler('/tmp/backup_scheduler_path', database,
                                           backup_scheduler_recv, backup_scheduler_send)
        self.p = Process(target=backup_scheduler)
        self.p.start()

    def tearDown(self) -> None:
        if self.p.is_alive():
            self.p.terminate()
        shutil.rmtree('/tmp/disk_db_concus', ignore_errors=True)
        shutil.rmtree('/tmp/backup_scheduler_path', ignore_errors=True)
        node_handler_process.NodeHandlerProcess = self.node_handler

    def test_simple_add_node(self):
        self.client_listener_send.send(('add_node', {'name': 'prueba',
                                                     'address': '127.0.0.1',
                                                     'port': 8080}))
        message, data = self.client_listener_recv.recv()
        self.assertEqual(message, "OK")
        self.p.terminate()
        database = DiskDatabase('/tmp/disk_db_concus')
        self.assertEqual(database.get_node_names(), {'prueba'})
        self.assertEqual(database.get_node_address('prueba'),
                         ('127.0.0.1', 8080))

    def test_simple_add_task_and_check_done(self):
        self.client_listener_send.send(('add_node', {'name': 'prueba',
                                                     'address': '127.0.0.1',
                                                     'port': 8080}))
        message, data = self.client_listener_recv.recv()
        self.assertEqual(message, "OK")
        self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                          'path': '/path'}))
        message, data = self.client_listener_recv.recv()
        self.assertEqual(message, "OK")
        self.assertEqual(len(data), 0)
        self.client_listener_send.send(('add_task', {'name': 'prueba',
                                                     'path': '/path',
                                                     'frequency': 1}))
        message, data = self.client_listener_recv.recv()
        self.assertEqual(message, "OK")
        self.barrier.wait(timeout=60)
        data = []
        while not data:
            self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                              'path': '/path'}))
            message, data = self.client_listener_recv.recv()
            self.assertEqual(message, "OK")
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['kb_size'], 0)
        while len(data) == 1:
            self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                              'path': '/path'}))
            message, data = self.client_listener_recv.recv()
            self.assertEqual(message, "OK")
        self.assertEqual(len(data), 2)
