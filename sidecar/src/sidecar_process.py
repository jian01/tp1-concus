import json
import signal
import socket
from multiprocessing import Process

from backup_utils.backup_file import BackupFile
from backup_utils.blocking_socket_transferer import BlockingSocketTransferer
from backup_utils.multiprocess_logging import MultiprocessingLogger

TMP_BACKUP_PATH = "/tmp/%d"
DEFAULT_SOCKET_BUFFER_SIZE = 4096


class SidecarProcess:
    logger = MultiprocessingLogger.getLogger(__module__)

    def __init__(self, port, listen_backlog):
        self.backup_no = 0
        self.port = port
        self.listen_backlog = listen_backlog
        self.process_list = []

    def _gentle_exit(self, _, __):
        for p in self.process_list:
            p.join()

    def __call__(self):
        signal.signal(signal.SIGINT, self._gentle_exit)
        signal.signal(signal.SIGTERM, self._gentle_exit)
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', self.port))
        self._server_socket.listen(self.listen_backlog)
        while True:
            client_sock = self.__accept_new_connection()
            p = Process(target=self.__handle_client_connection, args=(client_sock, self.backup_no))
            p.start()
            client_sock.close()
            self.backup_no += 1
            self.backup_no = self.backup_no % self.listen_backlog
            self.process_list = [p for p in self.process_list if p.is_alive()] + [p]

    @staticmethod
    def __handle_client_connection(client_sock, backup_no: int):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        socket_transferer = BlockingSocketTransferer(client_sock)
        try:
            msg = socket_transferer.receive_plain_text()
            msg = json.loads(msg)
            path, previous_checksum = msg['path'], msg['checksum']
            SidecarProcess.logger.debug("Previous checksum for path %s is '%s'" % (path, previous_checksum))
        except (OSError, TimeoutError) as e:
            SidecarProcess.logger.exception("Error while reading socket %s: %s" % (client_sock, e))
            socket_transferer.abort()
            return
        try:
            backup_file = BackupFile.create_from_path(path, TMP_BACKUP_PATH % backup_no)
        except Exception:
            SidecarProcess.logger.exception("Error while making backup file")
            socket_transferer.abort()
            return
        file_checksum = backup_file.get_hash()
        if file_checksum == previous_checksum:
            SidecarProcess.logger.info("Previous checksum equals to actual data, skipping backup")
            socket_transferer.send_plain_text("SAME")
            socket_transferer.abort()
            return
        else:
            socket_transferer.send_plain_text("DIFF")
        try:
            socket_transferer.send_file(TMP_BACKUP_PATH % backup_no)
            SidecarProcess.logger.debug("Backup file sent")
            socket_transferer.send_plain_text(file_checksum)
        except Exception as e:
            SidecarProcess.logger.exception("Error while writing socket %s: %s" % (client_sock, e))
            socket_transferer.abort()
            return
        finally:
            socket_transferer.close()
        return

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        c, addr = self._server_socket.accept()
        SidecarProcess.logger.info("Accepting connection for backuping")
        return c
