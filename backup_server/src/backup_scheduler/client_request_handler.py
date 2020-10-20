from typing import Any, Dict, Tuple, Optional
from src.database.database import Database


class ClientRequestHandler:

	def __init__(self, database: Database):
		"""

		:param database: the database
		"""
		self.database = database

	def add_node(self, name: str, address: str, port: int) -> Tuple[Optional[Any], bool]:
		"""
		Adds a node to the database

		:param name: the node name
		:param address: the node address
		:param port: the node port
		:return: no data and a boolean indicating if the tasks have changed
		"""
		self.database.register_node(name, address, port)
		if self.database.get_tasks_for_node(name):
			return None, True
		return None, False

	def add_task(self, name: str, path: str, frequency: int) -> Tuple[Optional[Any], bool]:
		"""
		Adds a task to the node

		:param name: the name of the node
		:param path: the path inside the node
		:param frequency: the backup frequency in minutes
		:return: no data and a boolean indicating if the tasks have changed, always true
		"""
		self.database.add_scheduled_task(name, path, frequency)
		return None, True

	def query_backups(self, name: str, path: str) -> Tuple[Optional[Any], bool]:
		"""
		Queries the node backups

		:param name: the name of the node
		:param path: the path
		:return: a tuple with data and boolean indicating if the tasks have changed always false
		"""
		finished_tasks = self.database.get_node_finished_tasks(name, path)
		finished_tasks = [ft.to_dict() for ft in finished_tasks]
		return finished_tasks, False

	def parse_command(self, command: str, kwargs: Dict[str, Any]) -> Tuple[Optional[Any], bool]:
		"""
		Runs a command, if the command does not exist does nothing

		:param command: the command to run
		:param kwargs: the arguments for the command
		:return: a tuple with data and boolean indicating if the tasks have changed
		"""
		if command == "add_node":
			return self.add_node(**kwargs)
		elif command == "add_task":
			return self.add_task(**kwargs)
		elif command == "query_backups":
			return self.query_backups(**kwargs)
		else:
			raise Exception('Unexistent command')
