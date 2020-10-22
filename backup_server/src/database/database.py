from abc import abstractmethod
from typing import NoReturn, List, Set, Tuple

from src.database.entities.finished_task import FinishedTask


class Database:
    """
    Abstraction for the database
    """

    @abstractmethod
    def register_node(self, node_name: str, node_addr: str, node_port: int) -> NoReturn:
        """
        Adds a new node to the database, if theres another one with the same name
        it overrides keeping the scheduled tasks.

        :param node_name: the name of the node
        :param node_addr: the address of the node
        :param node_port: the port of the node for communication
        """

    @abstractmethod
    def get_node_names(self) -> Set[str]:
        """
        Gets all the node names

        :return: a set of node names
        """

    @abstractmethod
    def get_node_address(self, node_name: str) -> Tuple[str, int]:
        """
        Gets a nodes address

        :raises:
            UnexistentNodeError: if the node named 'node_name' is not registered

        :return: a tuple (address, port)
        """

    @abstractmethod
    def get_tasks_for_node(self, node_name: str) -> List[Tuple[str, int]]:
        """
        Gets the scheduled tasks of a node

        :param node_name: the name of the node
        :return: a list of tuples (node_path, frequency)
        """

    @abstractmethod
    def add_scheduled_task(self, node_name: str, node_path: str, frequency: int) -> NoReturn:
        """
        Adds a new scheduled task for the node

        :raises:
            UnexistentNodeError: if the node named 'node_name' is not registered

        :param node_name: the name of the node for which the task corresponds
        :param node_path: the path inside the node
        :param frequency: the frequency in minutes for the task
        """

    @abstractmethod
    def register_finished_task(self, node_name: str, node_path: str, task: FinishedTask) -> NoReturn:
        """
        Registers a finished task for the node name and path

        :raises:
            UnexistentNodeError: if the node or path do not exist

        :param node_name: the node name
        :param node_path: the node path
        :param task: the finished task
        """

    @abstractmethod
    def get_node_finished_tasks(self, node_name: str, node_path: str) -> List[FinishedTask]:
        """
        List all finished tasks for a node and path

        :param node_name: the node name
        :param node_path: the node path
        :return: a list of finished tasks ordered from most recent to latest
        """

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
