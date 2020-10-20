import socket
import logging
import json
from multiprocessing import Pipe


class ClientListener:
	def __init__(self, port, listen_backlog,
				 backup_scheduler_write: Pipe,
				 backup_scheduler_read: Pipe):
		self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self._server_socket.bind(('', port))
		self._server_socket.listen(listen_backlog)
		self.backup_scheduler_write = backup_scheduler_write
		self.backup_scheduler_read = backup_scheduler_read

	def __call__(self):
		while True:
			client_sock = self.__accept_new_connection()
			self.__handle_client_connection(client_sock)

	def __handle_client_connection(self, client_sock):
		"""
		Read message from a specific client socket and closes the socket

		If a problem arises in the communication with the client, the
		client socket will also be closed
		"""
		try:
			msg = client_sock.recv(2048).rstrip()
			parsed_msg = json.loads(msg)
			self.backup_scheduler_write.send((parsed_msg['command'], parsed_msg['args']))
			message, data = self.backup_scheduler_read.recv()
			client_sock.send(json.dumps({"message": message, "data": data}).encode("utf-8"))
		except OSError:
			logging.error("Error while reading socket {}".format(client_sock))
		finally:
			client_sock.close()

	def __accept_new_connection(self):
		"""
		Accept new connections

		Function blocks until a connection to a client is made.
		Then connection created is printed and returned
		"""

		logging.info("Accepting connection for client listening")
		c, addr = self._server_socket.accept()
		return c