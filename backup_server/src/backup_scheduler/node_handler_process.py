from typing import NoReturn


class NodeHandlerProcess:
    """
    Handles the connection with the node for backuping
    """

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

    def run(self) -> NoReturn:
        """
        Code for running the handler in a new process

        The process works this way:
            1. Connects to node sidecar asking for node_path compressed
            2. Downloads the file saving it in write file path
                2.1. At start it writes an empty file named self.write_file_path but ending with .WIP
                2.2. Starts saving the backup in a file located in self.write_file_path
                2.3. When the backup is saved saves an empty file named self.write_file_path but ending with .CORRECT
                2.4. Deletes the .WIP file
            3. Ｓｅｐｐｕｋｕ, the process kills itself
        """
        return