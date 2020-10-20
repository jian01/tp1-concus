import socket
import logging
from multiprocessing import Process

class SidecarProcess:
	def __init__(self, port, listen_backlog):
		self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self._server_socket.bind(('', port))
		self._server_socket.listen(listen_backlog)

	def __call__(self):
		process_list = []
		while True:
			client_sock = self.__accept_new_connection()
			p = Process(target=self.__handle_client_connection, args=(client_sock,))
			p.start()
			process_list = [p for p in process_list if p.is_alive()] + [p]

	@staticmethod
	def __handle_client_connection(client_sock):
		"""
		Read message from a specific client socket and closes the socket

		If a problem arises in the communication with the client, the
		client socket will also be closed
		"""
		try:
			msg = client_sock.recv(1024).rstrip()
			with open(msg, "rb") as file:
				client_sock.send(file.read())
		except OSError as e:
			logging.error("Error while reading socket %s with error %s" % (client_sock, e))
		finally:
			client_sock.close()

	def __accept_new_connection(self):
		"""
		Accept new connections

		Function blocks until a connection to a client is made.
		Then connection created is printed and returned
		"""

		logging.info("Accepting connection for backuping")
		c, addr = self._server_socket.accept()
		return c