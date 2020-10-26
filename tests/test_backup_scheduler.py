import os
import shutil
import unittest
from multiprocessing import Process, Pipe, Barrier

from backup_utils.backup_file import BackupFile

from src.backup_scheduler import node_handler_process
from src.database.disk_database import DiskDatabase
from threading import BrokenBarrierError


class MockNodeHandler:
    BARRIER = None
    PATH_TO_BACKUP = '/tmp/data_for_backup'

    def __init__(self, node_address: str, node_port: int,
                 node_path: str, write_file_path: str, previous_checksum: str):
        self.node_address = node_address
        self.node_port = node_port
        self.node_path = node_path
        self.write_file_path = write_file_path
        self.previous_checksum = previous_checksum

    def __call__(self, *args, **kwargs):
        MockNodeHandler.BARRIER.wait()
        bf = BackupFile.create_from_path(MockNodeHandler.PATH_TO_BACKUP,
                                         self.write_file_path)
        if bf.get_hash() == self.previous_checksum:
            open(self.write_file_path + ".SAME", "w").close()
        else:
            open(self.write_file_path + ".CORRECT", "w").close()


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
        shutil.rmtree('/tmp/data_for_backup', ignore_errors=True)
        os.mkdir('/tmp/data_for_backup')
        with open('/tmp/data_for_backup/data', 'w') as data_file:
            data_file.write("adasdsa")
        database = DiskDatabase('/tmp/disk_db_concus')
        shutil.rmtree('/tmp/backup_scheduler_path', ignore_errors=True)
        os.mkdir('/tmp/backup_scheduler_path')
        with open('/tmp/backup_scheduler_path/trash', 'w') as trash_file:
            trash_file.write("trash")
        backup_scheduler_recv, self.client_listener_send = Pipe(False)
        self.client_listener_recv, backup_scheduler_send = Pipe(False)
        backup_scheduler = BackupScheduler('/tmp/backup_scheduler_path', database,
                                           backup_scheduler_recv, backup_scheduler_send, 10)
        self.p = Process(target=backup_scheduler)
        self.p.start()

    def tearDown(self) -> None:
        if self.p.is_alive():
            self.p.terminate()
        shutil.rmtree('/tmp/disk_db_concus', ignore_errors=True)
        shutil.rmtree('/tmp/backup_scheduler_path', ignore_errors=True)
        shutil.rmtree('/tmp/data_for_backup', ignore_errors=True)
        node_handler_process.NodeHandlerProcess = self.node_handler
        self.client_listener_send.close()

    def test_cleanup_at_start(self):
        self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                          'path': '/path'}))
        message, data = self.client_listener_recv.recv()
        self.assertEqual(message, "OK")
        self.assertFalse(os.path.exists('/tmp/backup_scheduler_path/trash'))

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

    def test_simple_add_task_and_check_done_same_data(self):
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
        self.barrier.wait()
        data = []
        while not data:
            self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                              'path': '/path'}))
            message, data = self.client_listener_recv.recv()
            self.assertEqual(message, "OK")
        self.assertEqual(len(data), 1)
        self.assertTrue(data[0]['kb_size'] > 0)
        self.barrier.wait()
        while len(data) == 1:
            self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                              'path': '/path'}))
            message, data = self.client_listener_recv.recv()
            self.assertEqual(message, "OK")
        self.assertEqual(len(data), 2)
        self.assertEqual(len(set([d['result_path'] for d in data])), 1)

    def test_simple_add_task_and_check_done_different_data(self):
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
        self.barrier.wait()
        data = []
        while not data:
            self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                              'path': '/path'}))
            message, data = self.client_listener_recv.recv()
            self.assertEqual(message, "OK")
        self.assertEqual(len(data), 1)
        self.assertTrue(data[0]['kb_size'] > 0)
        with open('/tmp/data_for_backup/data', 'w') as data_file:
            data_file.write("Lorem Ipsum dolor sit amet")
        self.barrier.wait()
        while len(data) == 1:
            self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                              'path': '/path'}))
            message, data = self.client_listener_recv.recv()
            self.assertEqual(message, "OK")
        self.assertEqual(len(data), 2)
        self.assertEqual(len(set([d['result_path'] for d in data])), 2)
        self.assertTrue(len(os.listdir('/tmp/backup_scheduler_path')) >= 2)

    def test_simple_add_task_and_delete_task(self):
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
        self.barrier.wait()
        data = []
        while not data:
            self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                              'path': '/path'}))
            message, data = self.client_listener_recv.recv()
            self.assertEqual(message, "OK")
        self.assertEqual(len(data), 1)
        self.assertTrue(data[0]['kb_size'] > 0)
        with open('/tmp/data_for_backup/data', 'w') as data_file:
            data_file.write("Lorem Ipsum dolor sit amet")
        self.client_listener_send.send(('delete_scheduled_task',
                                        {'name': 'prueba', 'path': '/path'}))
        message, data = self.client_listener_recv.recv()
        self.assertEqual(message, "OK")
        with self.assertRaises(BrokenBarrierError):
            self.barrier.wait(timeout=30.0)
        self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                          'path': '/path'}))
        message, data = self.client_listener_recv.recv()
        self.assertEqual(message, "OK")
        self.assertEqual(len(data), 1)

    def test_simple_add_task_and_delete_node(self):
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
        self.barrier.wait()
        data = []
        while not data:
            self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                              'path': '/path'}))
            message, data = self.client_listener_recv.recv()
            self.assertEqual(message, "OK")
        self.assertEqual(len(data), 1)
        self.assertTrue(data[0]['kb_size'] > 0)
        with open('/tmp/data_for_backup/data', 'w') as data_file:
            data_file.write("Lorem Ipsum dolor sit amet")
        self.client_listener_send.send(('delete_node', {'name': 'prueba'}))
        message, data = self.client_listener_recv.recv()
        self.assertEqual(message, "OK")
        self.client_listener_send.send(('query_backups', {'name': 'prueba',
                                                          'path': '/path'}))
        message, data = self.client_listener_recv.recv()
        self.assertEqual(message, "OK")
        self.assertEqual(len(data), 0)