import unittest
import os
import shutil
from src.database.disk_database import DiskDatabase
from src.database.exceptions.unexistent_node_error import UnexistentNodeError
from src.database.entities.finished_task import FinishedTask
from datetime import datetime


class TestDiskDatabase(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree('/tmp/disk_db_concus', ignore_errors=True)
        os.mkdir('/tmp/disk_db_concus')
        self.database = DiskDatabase('/tmp/disk_db_concus')

    def tearDown(self) -> None:
        shutil.rmtree('/tmp/disk_db_concus', ignore_errors=True)

    def test_register_one_node(self):
        self.database.register_node('name', 'address', 1111)
        self.assertEqual(self.database.get_node_names(), {'name'})

    def test_register_one_node_and_load(self):
        self.database.register_node('name', 'address', 1111)
        self.database = DiskDatabase('/tmp/disk_db_concus')
        self.assertEqual(self.database.get_node_names(), {'name'})

    def test_register_nodes_and_load(self):
        assert_set = set()
        for i in range(250):
            self.database.register_node('node_%d' % i, 'address', 1111)
            assert_set.update(['node_%d' % i])
        self.assertEqual(self.database.get_node_names(), assert_set)
        self.database = DiskDatabase('/tmp/disk_db_concus')
        self.assertEqual(self.database.get_node_names(), assert_set)

    def test_get_node_address(self):
        for i in range(250):
            self.database.register_node('node', '%d' % i, i)
            self.assertEqual(self.database.get_node_address('node'), ('%d' % i, i))
        self.database = DiskDatabase('/tmp/disk_db_concus')
        self.assertEqual(self.database.get_node_address('node'), ('%d' % 249, 249))

    def test_get_node_address_error(self):
        self.database.register_node('name', 'address', 1111)
        with self.assertRaises(UnexistentNodeError):
            self.database.get_node_address('name2')

    def test_add_one_scheduled_task(self):
        self.database.register_node('node', 'address', 1111)
        self.database.add_scheduled_task('node', '/home', 3)
        self.assertEqual(self.database.get_tasks_for_node('node'), [('/home', 3)])

    def test_add_scheduled_task_error(self):
        self.database.register_node('node', 'address', 1111)
        with self.assertRaises(UnexistentNodeError):
            self.database.add_scheduled_task('node2', '/etc', 5)
        self.database.add_scheduled_task('node', '/home', 3)
        self.assertEqual(self.database.get_tasks_for_node('node'), [('/home', 3)])
        self.assertEqual(self.database.get_tasks_for_node('node2'), [])

    def test_add_scheduled_task_override(self):
        self.database.register_node('node', 'address', 1111)
        self.database.add_scheduled_task('node', '/home', 3)
        self.database.add_scheduled_task('node', '/home', 5)
        self.assertEqual(self.database.get_tasks_for_node('node'), [('/home', 5)])

    def test_add_scheduled_tasks_same_node(self):
        self.database.register_node('node', 'address', 1111)
        self.database.add_scheduled_task('node', '/home', 3)
        self.database.add_scheduled_task('node', '/etc', 5)
        self.assertEqual(self.database.get_tasks_for_node('node'), [('/home', 3), ('/etc', 5)])

    def test_recover_each_step_multiple_scheduled_tasks_multiple_nodes(self):
        for i in range(324):
            self.database.register_node('node%d' % i, 'address', 1111)
            self.database = DiskDatabase('/tmp/disk_db_concus')
            for j in range(4):
                self.database.add_scheduled_task('node%d' % i, '/%d' % j, j)
                self.database = DiskDatabase('/tmp/disk_db_concus')
        for i in range(324):
            self.database = DiskDatabase('/tmp/disk_db_concus')
            self.assertEqual(self.database.get_tasks_for_node('node%d' % i),
                             [('/0', 0), ('/1', 1), ('/2', 2), ('/3', 3)])

    def test_recover_after_add_node_multiple_scheduled_tasks_multiple_nodes(self):
        for i in range(324):
            self.database.register_node('node%d' % i, 'address', 1111)
            self.database = DiskDatabase('/tmp/disk_db_concus')
            for j in range(4):
                self.database.add_scheduled_task('node%d' % i, '/%d' % j, j)
        for i in range(324):
            self.assertEqual(self.database.get_tasks_for_node('node%d' % i),
                             [('/0', 0), ('/1', 1), ('/2', 2), ('/3', 3)])

    def test_recover_after_add_task_multiple_scheduled_tasks_multiple_nodes(self):
        for i in range(324):
            self.database.register_node('node%d' % i, 'address', 1111)
            for j in range(4):
                self.database.add_scheduled_task('node%d' % i, '/%d' % j, j)
                self.database = DiskDatabase('/tmp/disk_db_concus')
        for i in range(324):
            self.assertEqual(self.database.get_tasks_for_node('node%d' % i),
                             [('/0', 0), ('/1', 1), ('/2', 2), ('/3', 3)])

    def test_add_one_finished_task(self):
        self.database.register_node('node', 'address', 1111)
        self.database.add_scheduled_task('node', '/home', 4)
        ft1 = FinishedTask('/tmp/backup1', 223.43, datetime.now())
        self.database.register_finished_task('node', '/home', ft1)
        self.assertEqual(self.database.get_node_finished_tasks('node', '/home'), [ft1])

    def test_add_finished_task_errors(self):
        self.database.register_node('node', 'address', 1111)
        self.database.add_scheduled_task('node', '/home', 4)
        ft1 = FinishedTask('/tmp/backup1', 223.43, datetime.now())
        with self.assertRaises(UnexistentNodeError):
            self.database.register_finished_task('node', '/home2', ft1)
        with self.assertRaises(UnexistentNodeError):
            self.database.register_finished_task('node1', '/home', ft1)

    def test_recover_each_step_add_finished_tasks(self):
        self.database.register_node('node', 'address', 1111)
        self.database.add_scheduled_task('node', '/home', 4)
        tasks = []
        for i in range(250):
            ft = FinishedTask('/tmp/backup1', 223.43, datetime.now())
            tasks.insert(0, ft)
            self.database.register_finished_task('node', '/home', ft)
            self.database = DiskDatabase('/tmp/disk_db_concus')
        self.assertEqual(self.database.get_node_finished_tasks('node', '/home'), tasks[:10])

    def test_recover_2n_steps_add_finished_tasks(self):
        self.database.register_node('node', 'address', 1111)
        self.database.add_scheduled_task('node', '/home', 4)
        tasks = []
        for i in range(250):
            ft = FinishedTask('/tmp/backup1', 223.43, datetime.now())
            tasks.insert(0, ft)
            self.database.register_finished_task('node', '/home', ft)
            if i % 2 == 0:
                self.database = DiskDatabase('/tmp/disk_db_concus')
        self.assertEqual(self.database.get_node_finished_tasks('node', '/home'), tasks[:10])
