import unittest
from datetime import datetime

from src.database.entities.finished_task import FinishedTask


class TestFinishedTasks(unittest.TestCase):
    def testToDictFromDict(self):
        ft = FinishedTask('/home', 0, datetime.now(), checksum="")
        self.assertEqual(ft, FinishedTask.from_dict(ft.to_dict()))
