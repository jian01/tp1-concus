import json
import os
import socket
from typing import NoReturn

from backup_utils.backup_file import BackupFile
from backup_utils.blocking_socket_transferer import BlockingSocketTransferer
from backup_utils.multiprocess_logging import MultiprocessingLogger

CORRECT_FILE_FORMAT = '%s.CORRECT'
WIP_FILE_FORMAT = '%s.WIP'
SAME_FILE_FORMAT = '%s.SAME'


class NodeHandlerProcess:
    """
    Handles the connection with the node for backuping
    """
    logger = MultiprocessingLogger.getLogger(__module__)

    def __init__(self, node_address: str, node_port: int,
                 node_path: str, write_file_path: str,
                 previous_checksum: str):
        """
        Creates a node handler process

        :param node_address: the address of the node
        :param node_port: the port of the node
        :param node_path: the path on the node to backup
        :param write_file_path: the local path where to save the backup
        :param previous_checksum: the previous backup checksum
        """
        self.node_address = node_address
        self.node_port = node_port
        self.node_path = node_path
        self.write_file_path = write_file_path
        self.previous_checksum = previous_checksum

    def __call__(self) -> NoReturn:
        """
        Code for running the handler in a new process

        The process works this way:
            1. Connects to node sidecar asking for node_path compressed
            2. If the backup is the same as previous checksum, writes a .SAME file
            3. Downloads the file saving it in write file path
                3.1. At start it writes an empty file named self.write_file_path but ending with .WIP
                3.2. Starts saving the backup in a file located in self.write_file_path
                3.3. When the backup is saved saves an empty file named self.write_file_path but ending with .CORRECT
                3.4. Deletes the .WIP file
            4. Ｓｅｐｐｕｋｕ
        """
        NodeHandlerProcess.logger.debug("Starting node handler for node %s:%d and path %s" %
                                        (self.node_address, self.node_port, self.node_path))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.node_address, self.node_port))
            socket_transferer = BlockingSocketTransferer(sock)
            socket_transferer.send_plain_text(json.dumps({"checksum": self.previous_checksum,
                                                          "path": self.node_path}))
        except OSError as e:
            NodeHandlerProcess.logger.exception("Error while writing socket %s: %s" % (sock, e))
            NodeHandlerProcess.logger.info("Terminating handler for node %s:%d and path %s" %
                                           (self.node_address, self.node_port, self.node_path))
            return
        msg = socket_transferer.receive_plain_text()
        if msg == "SAME":
            NodeHandlerProcess.logger.debug("The backup was the same")
            NodeHandlerProcess.logger.info("Terminating handler for node %s:%d and path %s" %
                                           (self.node_address, self.node_port, self.node_path))
            open(SAME_FILE_FORMAT % self.write_file_path, 'w').close()
            return
        if msg == "ABORT":
            NodeHandlerProcess.logger.error("Abort order sent from sidecar")
            NodeHandlerProcess.logger.info("Terminating handler for node %s:%d and path %s" %
                                           (self.node_address, self.node_port, self.node_path))
            return
        open(WIP_FILE_FORMAT % self.write_file_path, 'w').close()
        data_file = open(self.write_file_path, 'ab')
        try:
            socket_transferer.receive_file_data(data_file)
            NodeHandlerProcess.logger.debug("File data received")
            checksum = socket_transferer.receive_plain_text()
        except OSError as e:
            NodeHandlerProcess.logger.exception("Error while reading socket %s: %s" % (sock, e))
            NodeHandlerProcess.logger.info("Terminating handler for node %s:%d and path %s" %
                                           (self.node_address, self.node_port, self.node_path))
            return
        data_file.close()
        backup_file = BackupFile(self.write_file_path)
        if backup_file.get_hash() == checksum:
            NodeHandlerProcess.logger.debug("Backup checksum: %s" % checksum)
        else:
            NodeHandlerProcess.logger.error("Error verifying checksum. Local: %s vs Server: %s" %
                                            (backup_file.get_hash(), checksum))
            return
        open(CORRECT_FILE_FORMAT % self.write_file_path, 'w').close()
        os.remove(WIP_FILE_FORMAT % self.write_file_path)
        NodeHandlerProcess.logger.info("Terminating handler for node %s:%d and path %s" %
                                       (self.node_address, self.node_port, self.node_path))
