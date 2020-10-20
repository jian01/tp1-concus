import os
from multiprocessing import Pipe, Process
from src.database.disk_database import DiskDatabase
from src.backup_scheduler.backup_scheduler import BackupScheduler
from src.client_listener.client_listener import ClientListener

port = int(os.getenv('PORT'))
listen_backlog = int(os.getenv('LISTEN_BACKLOG'))
database_path = '/backup_data/database'

backup_scheduler_recv, client_listener_send = Pipe(False)
client_listener_recv, backup_scheduler_send = Pipe(False)


database = DiskDatabase(database_path)
backup_scheduler = BackupScheduler('/backup_data/data',
								   database, backup_scheduler_recv,
								   backup_scheduler_send)
client_listener = ClientListener(port, listen_backlog,
								 client_listener_send, client_listener_recv)
p = Process(target=client_listener)
p.start()
backup_scheduler()