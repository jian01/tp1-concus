import unittest
import os
import shutil
from sidecar.src.sidecar_process import SidecarProcess
from backup_server.src.backup_scheduler.node_handler_process import NodeHandlerProcess
from multiprocessing import Process


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
		shutil.rmtree('/tmp/backup_output', ignore_errors=True)
		self.p.terminate()

	def test_download_py(self):
		node_handler_process = NodeHandlerProcess('localhost', 1234,
												  'sidecar/src/sidecar_process.py',
												  '/tmp/backup_output/out')
		node_handler_process = Process(target=node_handler_process)
		node_handler_process.start()
		node_handler_process.join()
		backup_file = open('/tmp/backup_output/out', "rb")
		original_file = open('sidecar/src/sidecar_process.py', "rb")
		self.assertEqual(original_file.read(), backup_file.read())
		backup_file.close()
		original_file.close()

