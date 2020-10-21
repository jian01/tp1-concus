import tarfile
import hashlib

HASH_READ_BUF_SIZE = 1000000


class BackupFile:
	def __init__(self, tarfile_path: str=None):
		self.tarfile_path = tarfile_path
		self.calculated_hash = None

	@classmethod
	def create_from_path(cls, src_path: str, dst_path: str) -> 'BackupFile':
		"""
		Creates a new backup file in dst_path with the contents of src_path

		:param src_path: the source path
		:param dst_path: the destination path
		:return: a BackupFile object
		"""
		with tarfile.open(dst_path, mode='w:gz') as archive:
			archive.add(src_path, recursive=True)
		return cls(dst_path)

	def get_hash(self) -> str:
		"""
		Gets a hash of the file

		:return: a hash of the file
		"""
		assert self.tarfile_path
		if self.calculated_hash:
			return self.calculated_hash
		sha256 = hashlib.sha256()
		with open(self.tarfile_path, 'rb') as f:
			while True:
				data = f.read(HASH_READ_BUF_SIZE)
				if not data:
					break
				sha256.update(data)
		self.calculated_hash = sha256.hexdigest()
		return sha256.hexdigest()