from typing import NoReturn, List, Dict, Tuple, Set
from abc import abstractmethod
from .database import Database
from src.database.entities.finished_task import FinishedTask
from .exceptions.unexistent_node_error import UnexistentNodeError
import json
import pickle
import os

COMMIT_KEYWORD = "COMMIT"
MAX_LOGSIZE = 4000
MAX_UNCOMMITED = 200
MAX_FINISHED_TASKS = 10


class DiskDatabase(Database):
    """
    Disk database

    Estimados sres distribuidos I:
    La intencion es que si se cae el server cuando levanta recupera desde el mismo estado.
    Usa un writeahead log para no escribir el dict entero en cada operacion o para no tener que particionarlo.
        'Premature optimization is the root of all evil' -Donald Knuth
                                ｖｓ
        'Es divertido hacer un writeahead log' -Gianmarco Cafferata

    TODO: max logsize apply restriction
    TODO: use a template for %s\n and constants for dicts names and some keywords
    """

    @staticmethod
    def dump_db(database_path: str, database_data: Dict):
        """
        Dumps the database data

        :param database_path: the database path
        :param database_data: the database data to dump
        """
        with open("%s/database" % database_path, "wb") as database_file:
            pickle.dump(database_data, database_file)

    def load_database(self, database_path: str) -> NoReturn:
        """
        Loads the database data from the path

        :param database_path: the path where the database is stores
        """
        if os.path.exists("%s/database" % database_path):
            with open("%s/database" % database_path, "rb") as database_file:
                self.database = pickle.load(database_file)
        redo_operations = []
        self.logsize = 0
        if os.path.exists("%s/log" % database_path):
            with open("%s/log" % database_path, "r") as logfile:
                line = logfile.readline()
                while line:
                    self.logsize += 1
                    if line[:len(COMMIT_KEYWORD)] == COMMIT_KEYWORD:
                        redo_operations = []
                    else:
                        redo_operations.append(line)
                    line = logfile.readline()
            if redo_operations:
                for op in redo_operations:
                    try:
                        self._write_operation(**json.loads(op))
                    except UnexistentNodeError:
                        pass
                self.dump_db(database_path, self.database)
                with open("%s/log" % database_path, "a") as log:
                    log.write(COMMIT_KEYWORD + '\n')
                    log.flush()
            if self.logsize > 4000:
                open("%s/log" % database_path, 'w').close()

    def __init__(self, database_path: str):
        """
        Initializes the database

        :param database_path: the path where the database wis stored
        """
        self.database_path = database_path
        self.database = {}
        self.logsize = 0
        self.load_database(database_path)
        self.uncommited_size = 0
        self.writeahed_log = open("%s/log" % database_path, "a")

    def _write_operation(self, func: str, params: List, use_log: bool = False):
        if use_log:
            self.writeahed_log.write(json.dumps({'func': func,
                                                 'params': params}) + '\n')
            self.writeahed_log.flush()
            self.logsize += 1
        getattr(self, func)(self.database, *params)
        if use_log:
            self.uncommited_size += 1
            if self.uncommited_size > MAX_UNCOMMITED:
                self.dump_db(self.database_path, self.database)
                self.writeahed_log.write(COMMIT_KEYWORD + '\n')
                self.writeahed_log.flush()
                self.logsize += 1
                self.uncommited_size = 0

    @staticmethod
    def _register_node(database, node_name, node_addr, node_port):
        if node_name in database:
            database[node_name].update({'port': node_port, 'address': node_addr})
        else:
            database[node_name] = {'port': node_port, 'address': node_addr,
                                   'tasks': [], 'finished_tasks': []}

    def register_node(self, node_name: str, node_addr: str, node_port: int) -> NoReturn:
        """
        Adds a new node to the database, if theres another one with the same name
        it overrides keeping the scheduled tasks.

        :param node_name: the name of the node
        :param node_addr: the address of the node
        :param node_port: the port of the node for communication
        """
        self._write_operation('_register_node', [node_name, node_addr, node_port],
                              use_log=True)

    def get_node_names(self) -> Set[str]:
        """
        Gets all the node names

        :return: a set of node names
        """
        return set(self.database.keys())

    def get_node_address(self, node_name: str) -> Tuple[str, int]:
        """
        Gets a nodes address

        :raises:
            UnexistentNodeError: if the node named 'node_name' is not registered

        :return: a tuple (address, port)
        """
        if node_name not in self.database:
            raise UnexistentNodeError
        return self.database[node_name]['address'], self.database[node_name]['port']

    def get_tasks_for_node(self, node_name: str) -> List[Tuple[str, int]]:
        """
        Gets the scheduled tasks of a node

        :param node_name: the name of the node
        :return: a list of tuples (node_path, frequency)
        """
        if node_name not in self.database:
            return []
        return self.database[node_name]['tasks']

    @staticmethod
    def _add_scheduled_task(database, node_name, node_path, frequency):
        if node_name not in database:
            raise UnexistentNodeError
        database[node_name]['tasks'] = [t for t in database[node_name]['tasks'] if t[0] != node_path]
        database[node_name]['tasks'].append((node_path, frequency))

    def add_scheduled_task(self, node_name: str, node_path: str, frequency: int) -> NoReturn:
        """
        Adds a new scheduled task for the node

        If the path is already in a task the frequency will we overriden

        :raises:
            UnexistentNodeError: if the node named 'node_name' is not registered

        :param node_name: the name of the node for which the task corresponds
        :param node_path: the path inside the node
        :param frequency: the frequency in minutes for the task
        """
        self._write_operation('_add_scheduled_task', [node_name, node_path, frequency],
                              use_log=True)

    @staticmethod
    def _register_finished_task(database, node_name, node_path, task_data):
        if node_name not in database \
                or len([t for t in database[node_name]['tasks'] if t[0] == node_path]) == 0:
            raise UnexistentNodeError
        if task_data in database[node_name]['finished_tasks']:
            return
        database[node_name]['finished_tasks'].insert(0, task_data)
        if len(database[node_name]['finished_tasks']) > 10:
            database[node_name]['finished_tasks'] = database[node_name]['finished_tasks'][:10]

    def register_finished_task(self, node_name: str, node_path: str, task: FinishedTask) -> NoReturn:
        """
        Registers a finished task for the node name and path

        :raises:
            UnexistentNodeError: if the node or path do not exist

        :param node_name: the node name
        :param node_path: the node path
        :param task: the finished task
        """
        self._write_operation('_register_finished_task', [node_name, node_path, task.to_dict()],
                              use_log=True)

    def get_node_finished_tasks(self, node_name: str, node_path: str) -> List[FinishedTask]:
        """
        List all finished tasks for a node and path

        :param node_name: the node name
        :param node_path: the node path
        :return: a list of finished tasks ordered from most recent to latest
        """
        if node_name not in self.database:
            return []
        return [FinishedTask.from_dict(ft) for ft in self.database[node_name]['finished_tasks']]

    @abstractmethod
    def delete_scheduled_task(self, node_name: str, node_path: str) -> NoReturn:
        """
        Deletes a scheduled task for a node.

        If the node or the task does not exist it wont fail.

        :param node_name: the node name
        :param node_path: the node path of the task
        """

    @abstractmethod
    def delete_node(self, node_name: str) -> NoReturn:
        """
        Deletes a node

        If the node does not exist it wont fail

        :param node_name: the node name
        """