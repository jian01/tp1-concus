import json
import os
import socket
from multiprocessing import Process

from backup_utils.backup_file import BackupFile
from backup_utils.multiprocess_logging import MultiprocessingLogger

TMP_BACKUP_PATH = "/tmp/%d"
DEFAULT_SOCKET_BUFFER_SIZE = 4096


class SidecarProcess:
    logger = MultiprocessingLogger.getLogger(__module__)

    def __init__(self, port, listen_backlog):
        self.backup_no = 0
        self.port = port
        self.listen_backlog = listen_backlog

    def __call__(self):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', self.port))
        self._server_socket.listen(self.listen_backlog)
        process_list = []
        while True:
            client_sock = self.__accept_new_connection()
            p = Process(target=self.__handle_client_connection, args=(client_sock, self.backup_no))
            p.start()
            self.backup_no += 1
            process_list = [p for p in process_list if p.is_alive()] + [p]

    @staticmethod
    def __handle_client_connection(client_sock, backup_no: int):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """

        def _send_file_to(filename, sock):
            file_size = os.stat(filename).st_size
            sock.sendall(str(file_size).encode('utf-8'))
            sock.recv(1024)
            with open(filename, "rb") as file:
                while file_size > 0:
                    buffer = file.read(DEFAULT_SOCKET_BUFFER_SIZE)
                    sock.sendall(buffer)
                    file_size -= DEFAULT_SOCKET_BUFFER_SIZE

        try:
            msg = client_sock.recv(1024).rstrip()
            msg = json.loads(msg)
            path, previous_checksum = msg['path'], msg['checksum']
            SidecarProcess.logger.debug("Previous checksum for path %s is '%s'" % (path, previous_checksum))
        except OSError as e:
            SidecarProcess.logger.exception("Error while reading socket %s: %s" % (client_sock, e))
            client_sock.close()
            return
        backup_file = BackupFile.create_from_path(path, TMP_BACKUP_PATH % backup_no)
        file_checksum = backup_file.get_hash()
        if file_checksum == previous_checksum:
            SidecarProcess.logger.info("Previous checksum equals to actual data, skipping backup")
            client_sock.sendall("SAME".encode("utf-8"))
            client_sock.close()
            return
        else:
            client_sock.sendall("OK".encode("utf-8"))
        try:
            _send_file_to(TMP_BACKUP_PATH % backup_no, client_sock)
            SidecarProcess.logger.debug("Backup file sent")
            client_sock.recv(1024).rstrip()
            client_sock.sendall(file_checksum.encode("utf-8"))
        except OSError as e:
            SidecarProcess.logger.exception("Error while writing socket %s: %s" % (client_sock, e))
            return
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        c, addr = self._server_socket.accept()
        SidecarProcess.logger.info("Accepting connection for backuping")
        return c
