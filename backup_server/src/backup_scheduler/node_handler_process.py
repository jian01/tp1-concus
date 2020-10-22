import json
import os
import socket
from typing import NoReturn

from backup_utils.backup_file import BackupFile
from backup_utils.multiprocess_logging import MultiprocessingLogger

DEFAULT_SOCKET_BUFFER_SIZE = 4096
CORRECT_FILE_FORMAT = '%s.CORRECT'
WIP_FILE_FORMAT = '%s.WIP'


class NodeHandlerProcess:
    """
    Handles the connection with the node for backuping
    """
    logger = MultiprocessingLogger.getLogger(__module__)

    def __init__(self, node_address: str, node_port: int,
                 node_path: str, write_file_path: str):
        """
        Creates a node handler process

        :param node_address: the address of the node
        :param node_port: the port of the node
        :param node_path: the path on the node to backup
        :param write_file_path: the local path where to save the backup
        """
        self.node_address = node_address
        self.node_port = node_port
        self.node_path = node_path
        self.write_file_path = write_file_path

    @staticmethod
    def _receive_file_in(sock, file):
        file_size = int(sock.recv(1024))
        sock.send("OK".encode('utf-8'))
        while file_size > 0:
            buffer = sock.recv(DEFAULT_SOCKET_BUFFER_SIZE)
            file.write(buffer)
            file_size -= len(buffer)

    def __call__(self, *args, **kwargs) -> NoReturn:
        """
        Code for running the handler in a new process

        The process works this way:
            1. Connects to node sidecar asking for node_path compressed
            2. Downloads the file saving it in write file path
                2.1. At start it writes an empty file named self.write_file_path but ending with .WIP
                2.2. Starts saving the backup in a file located in self.write_file_path
                2.3. When the backup is saved saves an empty file named self.write_file_path but ending with .CORRECT
                2.4. Deletes the .WIP file
            3. Ｓｅｐｐｕｋｕ
        """
        NodeHandlerProcess.logger.debug("Starting node handler for node %s:%d and path %s" %
                                        (self.node_address, self.node_port, self.node_path))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.node_address, self.node_port))
            sock.send(json.dumps({"checksum": "dummy_checksum",
                                  "path": self.node_path}).encode("utf-8"))
        except OSError as e:
            NodeHandlerProcess.logger.exception("Error while writing socket %s" % (sock,))
            NodeHandlerProcess.logger.info("Terminating handler for node %s:%d and path %s" %
                                           (self.node_address, self.node_port, self.node_path))
            return
        open(WIP_FILE_FORMAT % self.write_file_path, 'w').close()
        data_file = open(self.write_file_path, 'ab')
        try:
            self._receive_file_in(sock, data_file)
            NodeHandlerProcess.logger.debug("File data received")
            sock.send("OK".encode('utf-8'))
            checksum = sock.recv(DEFAULT_SOCKET_BUFFER_SIZE).rstrip()
        except OSError as e:
            NodeHandlerProcess.logger.exception("Error while reading socket %s" % (sock,))
            NodeHandlerProcess.logger.info("Terminating handler for node %s:%d and path %s" %
                                           (self.node_address, self.node_port, self.node_path))
            return
        data_file.close()
        backup_file = BackupFile(self.write_file_path)
        if backup_file.get_hash() == checksum.decode("utf-8"):
            NodeHandlerProcess.logger.debug("Backup checksum: %s" % checksum.decode("utf-8"))
        else:
            NodeHandlerProcess.logger.error("Error verifying checksum. Local: %s vs Server: %s" %
                                            (backup_file.get_hash(), checksum.decode("utf-8")))
        open(CORRECT_FILE_FORMAT % self.write_file_path, 'w').close()
        os.remove(WIP_FILE_FORMAT % self.write_file_path)
        NodeHandlerProcess.logger.info("Terminating handler for node %s:%d and path %s" %
                                       (self.node_address, self.node_port, self.node_path))
