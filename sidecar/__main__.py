import os
from src.sidecar_process import SidecarProcess

port = int(os.getenv('PORT'))
listen_backlog = int(os.getenv('LISTEN_BACKLOG'))

SidecarProcess(port, listen_backlog)()