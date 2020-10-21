from backup_utils.backup_file import BackupFile
import unittest
import os
import shutil


class TestBackupFile(unittest.TestCase):
	def setUp(self) -> None:
		shutil.rmtree('/tmp/test_path', ignore_errors=True)
		os.mkdir('/tmp/test_path')
		with open('/tmp/test_path/test_file', "w") as test_file:
			test_file.write("dummy text")

	def tearDown(self) -> None:
		shutil.rmtree('/tmp/test_path', ignore_errors=True)

	def test_load_and_create_same_hash(self):
		backup_file = BackupFile.create_from_path('/tmp/test_path', '/tmp/file.tgz')
		backup_file2 = BackupFile('/tmp/file.tgz')
		self.assertEqual(backup_file2.get_hash(), backup_file.get_hash())

