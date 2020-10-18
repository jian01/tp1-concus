from typing import NoReturn, NamedTuple, Optional
from multiprocessing import Pipe, Process
from src.database.database import Database
from src.backup_scheduler.node_handler_process import NodeHandlerProcess
from src.database.entities.finished_task import FinishedTask
from datetime import datetime, timezone
import os
import base64
from time import sleep


SECONDS_TO_MINUTES = 3600
WRITE_FILE_PATH_TEMPLATE = '%s/backup_%d_%s_%s'


class ScheduledTask(NamedTuple):
    node_name: str
    node_address: str
    node_port: int
    node_path: str
    frequency: int
    last_backup: Optional[datetime] = None


class BackupScheduler:
    """
    Backup scheduler
    """

    def _initialize_schedule(self):
        for node_name in self.database.get_node_names():
            node_address, node_port = self.database.get_node_address(node_name)
            for path, frequency in self.database.get_tasks_for_node(node_name):
                finished_tasks = self.database.get_node_finished_tasks(node_name, path)
                last_backup = (finished_tasks[0].timestamp if finished_tasks else None)
                self.schedule.append(ScheduledTask(node_name=node_name, node_address=node_address,
                                                   node_port=node_port, node_path=path,
                                                   frequency=frequency,
                                                   last_backup=last_backup))

    def _clean_backup_path(self):
        return

    def __init__(self, backup_path: str, database: Database,
                 pipe_request_read: Pipe, pipe_request_answer: Pipe):
        """
        Initializes the backup scheduler

        :param backup_path: the path where to make the backups
        :param database: the database to use
        :param pipe_request_read: the read end pipe to handle controller commands
        :param pipe_request_answer: the read end pipe to handle controller commands
        """
        self.backup_path = backup_path
        self.database = database
        self.pipe_request_read = pipe_request_read
        self.pipe_request_answer = pipe_request_answer
        self.schedule = []
        self.client_controller_process = None
        self.running_tasks = {}

    @staticmethod
    def safe_base64(text: str) -> str:
        """
        Generates a safe base64 for filenames according rfc3548
        :param text: the text to encode
        :return: the safe text
        """
        return base64.b64encode(bytes(text, 'ascii'), b'-_').decode('ascii')

    def run(self) -> NoReturn:
        """
        Code for running the main loop in the main process

        The process works this way, while true:
            1. Checks for 60s the pipe from the client controller to see if theres an order to execute
                1.1 If theres an order to execute it runs it
                1.2 The answer to order is sent through self.pipe_request_answer
            2. For each node handler process that ended:
                If there is a .CORRECT it deletes it and registers the backup in the database
                If there is no .CORRECT it deletes all files associated with that process backup
            3. Launches new node handler processes for the backups that need to be done according last
            backup time, actual time and if there isnt a backup already running for that node and path

        If the client controller pipe is broken:
            * Creates a new client controller

        If other error happens and the process must die:
            * Kill all other processes then dies
        """
        self._initialize_schedule()
        self._clean_backup_path()
        while True:
            sleep(60)  # wait for client controller orders
            actual_time = datetime.now()
            now_running_tasks = {}
            for node_data, run_data in self.running_tasks.items():
                if not run_data[1].is_alive():
                    if os.path.isfile('%s.CORRECT' % run_data[0]):
                        os.remove('%s.CORRECT' % run_data[0])
                        ft = FinishedTask(result_path=run_data[0],
                                          kb_size=os.path.getsize(run_data[0]) / 1024,
                                          timestamp=datetime.now())
                        self.database.register_finished_task(node_data[0], node_data[1], ft)
                    else:
                        if os.path.isfile(run_data[0]):
                            os.remove(run_data[0])
                        if os.path.isfile('%s.WIP' % run_data[0]):
                            os.remove('%s.WIP' % run_data[0])
                else:
                    now_running_tasks[node_data] = run_data
            for sched_task in self.schedule:
                if (sched_task.node_name, sched_task.node_path) in now_running_tasks:
                    continue
                if sched_task.last_backup and\
                        (actual_time - sched_task.last_backup).seconds / SECONDS_TO_MINUTES < sched_task.frequency:
                    continue
                write_file_path = WRITE_FILE_PATH_TEMPLATE % (self.backup_path,
                                                              actual_time.replace(tzinfo=timezone.utc).timestamp(),
                                                              sched_task.node_name,
                                                              self.safe_base64(sched_task.node_path))
                node_handler = NodeHandlerProcess(node_address=sched_task.node_address,
                                                  node_path=sched_task.node_path,
                                                  node_port=sched_task.node_port,
                                                  write_file_path=write_file_path)
                p = Process(target=node_handler.run)
                p.start()
                now_running_tasks[(sched_task.node_name, sched_task.node_path)] = (write_file_path, p)
            self.running_tasks = now_running_tasks