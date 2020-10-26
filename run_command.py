import socket
from backup_utils.blocking_socket_transferer import BlockingSocketTransferer
import argparse
import json

parser = argparse.ArgumentParser(description='Sends a command to backup server')
parser.add_argument('--port', required=True, type=int,
                    help='the port of the the server')
parser.add_argument('--address', required=True,
                    help='the address of the server')
parser.add_argument('--command', required=True,
                    help='the command to run')
parser.add_argument('--args', required=True,
                    help='the args dict')
args = parser.parse_args()

port = args.port
address = args.address
command = args.command
dict_arguments = json.loads(args.args)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((address, port))
socket_transferer = BlockingSocketTransferer(sock)
socket_transferer.send_plain_text(json.dumps({"command": command,
                                              "args": dict_arguments}))
msg = socket_transferer.receive_plain_text()
print(msg)
socket_transferer.close()