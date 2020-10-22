import logging.config
import os
from multiprocessing import Pipe, Process

from src.backup_scheduler.backup_scheduler import BackupScheduler
from src.client_listener.client_listener import ClientListener
from src.database.disk_database import DiskDatabase


def main() -> int:
    logging.config.fileConfig('log.conf', disable_existing_loggers=True)
    port = int(os.getenv('PORT'))
    listen_backlog = 10
    backup_data_path = os.getenv('BACKUP_DATA_PATH')

    backup_scheduler_recv, client_listener_send = Pipe(False)
    client_listener_recv, backup_scheduler_send = Pipe(False)

    database = DiskDatabase(backup_data_path + "/database")
    backup_scheduler = BackupScheduler(backup_data_path + "/data",
                                       database, backup_scheduler_recv,
                                       backup_scheduler_send)
    client_listener = ClientListener(port, listen_backlog,
                                     client_listener_send, client_listener_recv)
    p = Process(target=client_listener)
    p.start()
    backup_scheduler()
    p.terminate()
    return 1  # This function should never end


if __name__ == '__main__':
    main()
