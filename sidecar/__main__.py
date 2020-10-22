import logging.config
import os

from src.sidecar_process import SidecarProcess

logging.config.fileConfig('log.conf', disable_existing_loggers=True)

port = int(os.getenv('PORT'))
listen_backlog = 10

SidecarProcess(port, listen_backlog)()
