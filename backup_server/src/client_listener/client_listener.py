import json
import socket
from multiprocessing import Pipe

from backup_utils.multiprocess_logging import MultiprocessingLogger


class ClientListener:
    logger = MultiprocessingLogger.getLogger(__module__)

    def __init__(self, port, listen_backlog,
                 backup_scheduler_write: Pipe,
                 backup_scheduler_read: Pipe):
        self.port = port
        self.listen_backlog = listen_backlog
        self.backup_scheduler_write = backup_scheduler_write
        self.backup_scheduler_read = backup_scheduler_read
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', self.port))
        self._server_socket.listen(self.listen_backlog)

    def __call__(self):
        while True:
            client_sock = self.__accept_new_connection()
            ClientListener.logger.info("Client connection accepted")
            self.__handle_client_connection(client_sock)

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            msg = client_sock.recv(2048).rstrip()
        except OSError:
            ClientListener.logger.error("Error while reading socket {}".format(client_sock))
            client_sock.close()
            return
        parsed_msg = json.loads(msg)
        self.backup_scheduler_write.send((parsed_msg['command'], parsed_msg['args']))
        try:
            message, data = self.backup_scheduler_read.recv()
        except EOFError as e:
            ClientListener.logger.exception("Backup scheduler death")
            client_sock.close()
            raise e
        try:
            client_sock.sendall(json.dumps({"message": message, "data": data}).encode("utf-8"))
        except OSError:
            ClientListener.logger.exception("Error writing through socket")
            client_sock.close()
            return

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        c, addr = self._server_socket.accept()
        return c
