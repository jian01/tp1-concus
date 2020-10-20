import unittest
from src.database.entities.finished_task import FinishedTask
from datetime import datetime

class TestFinishedTasks(unittest.TestCase):
	def testToDictFromDict(self):
		ft = FinishedTask('/home', 0, datetime.now())
		self.assertEqual(ft, FinishedTask.from_dict(ft.to_dict()))