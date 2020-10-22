import os
import shutil
import unittest
from multiprocessing import Process

from backup_utils.backup_file import BackupFile

from backup_server.src.backup_scheduler.node_handler_process import NodeHandlerProcess
from sidecar.src.sidecar_process import SidecarProcess


class TestSidecar(unittest.TestCase):
    def setUp(self) -> None:
        try:
            from pytest_cov.embed import cleanup_on_sigterm
        except ImportError:
            pass
        else:
            cleanup_on_sigterm()
        shutil.rmtree('/tmp/backup_output', ignore_errors=True)
        os.mkdir('/tmp/backup_output')
        self.sidecar_process = SidecarProcess(1234, 5)
        self.p = Process(target=self.sidecar_process)
        self.p.start()

    def tearDown(self) -> None:
        if self.p.is_alive():
            self.p.terminate()
        shutil.rmtree('/tmp/backup_output', ignore_errors=True)

    def test_download_py(self):
        node_handler_process = NodeHandlerProcess('localhost', 1234,
                                                  'sidecar/src/sidecar_process.py',
                                                  '/tmp/backup_output/out')
        node_handler_process = Process(target=node_handler_process)
        node_handler_process.start()
        node_handler_process.join()
        expected_file = BackupFile.create_from_path('sidecar/src/sidecar_process.py', "/tmp/backup_output/out2")
        backup_file = BackupFile("/tmp/backup_output/out")
        self.assertEqual(expected_file.get_hash(), backup_file.get_hash())
