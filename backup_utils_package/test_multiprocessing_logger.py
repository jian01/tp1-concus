import logging
import os
import unittest
from time import sleep

from backup_utils.multiprocess_logging import MultiprocessingLogger


class TestMultiprocessingLogger(unittest.TestCase):
    def setUp(self):
        logger = logging.getLogger('root')
        fh = logging.FileHandler('/tmp/log', 'w')
        fh.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(fh)
        print(logger.handlers)
        self.logger = MultiprocessingLogger(logger)

    def tearDown(self) -> None:
        os.remove('/tmp/log')

    def test_log_and_read(self):
        self.logger.info("zaraza")
        sleep(2)
        logging.shutdown()
        with open('/tmp/log', 'r') as logfile:
            logifile_content = logfile.read()
            self.assertTrue("zaraza" in logifile_content)
