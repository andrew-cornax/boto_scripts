import os
import argparse
import base64
import boto3
import glacier_uploader
import tarfile
import time
import hashlib
import pickle


def scantree__(root_directory):
	scan = os.scandir(root_directory)
	for entry in scan:
		if entry.is_dir():
			yield from scantree__(entry.path)
		else:
			yield entry

def create_archive(file_list):
	archive_path = "/tmp/glacier_{0}.tar.bz2".format(time.strftime("%s"))
	with tarfile.open(archive_path, "w:bz2") as archive:
		for file in file_list:
			archive.add(file.path)
	return archive_path

def sha256sum(file_path):
	s = hashlib.sha256()
	with open(file_path, "rb") as file:
		for block in iter(lambda : file.read(65536), b''):
			s.update(block)

	return s.hexdigest()

class GlacierBackupEngine:

	def __init__(self, glacier, vault_name, glacier_metadata_file_path):
		self.glacier = glacier
		self.vault_name = vault_name
		self.glacier_metadata_file_path = glacier_metadata_file_path
		self.load_glacier_metadata()

	def load_glacier_metadata(self):
		if self.glacier_metadata_file_path and os.path.exists(self.glacier_metadata_file_path):
			with open(self.glacier_metadata_file_path, 'rb') as glacier_metadata_file:
				self.glacier_metadata = pickle.load(glacier_metadata_file)
		else:
			return {}

	def save_glacier_metadata(self):
		with open(self.glacier_metadata_file_path, 'wb') as glacier_metadata_file:
			pickle.dump(self.glacier_metadata, glacier_metadata_file)

	def backup_directory(self, directory):
		"""Upload the contents of a given directory to glacier as a single tar.bz2 archive. Note that only files are archived - subdirectories are ignored."""
		files_to_archive = [entry for entry in os.scandir(directory) if entry.is_file()]

		archive_path = create_archive(files_to_archive)
		archive_sum = sha256sum(archive_path)
		archive_description = str(base64.b64encode(directory.encode('ascii')))

		if not directory in self.glacier_metadata:
			msg = "Detected no metadata for the archive for {0} - Uploading the archive to glacier".format(directory)
			upload_archive = True
		elif archive_sum != self.glacier_metadata[directory]['sha256sum']:
			msg = "Detected new sha256 sum of the archive for {0} - Uploading the new archive to glacier, and deleting the old one.".format(directory)
			upload_archive = True
			glacier.delete_archive(vaultName=self.vault_name, archiveId=self.glacier_metadata[directory]['archive_id'])
		else:
			msg = "Sha256sum of the archive for {0} has not changed, so it will not be uploaded".format(directory)
			upload_archive = False

		print(msg)

		if upload_archive:
			glacier_upload = glacier_uploader.upload(self.glacier, self.vault_name, archive_path, archive_description)
			self.glacier_metadata[directory] = {
				'archive_id' : glacier_upload['archiveId'],
				'sha256sum' : archive_sum
			}


parser = argparse.ArgumentParser(description="Backup a directory to AWS glacier")
parser.add_argument('vault_name', metavar="vault name", type=str, help="name of the glacier vault to back up data to")
parser.add_argument('directory', metavar='directory', type=str, help="absolute path of the directory to back up")
parser.add_argument('--glacier_metadata_file_path', metavar='glacier metadata file', type=str, help='(optional) path to a file to read and store glacier archive metadata')
args = parser.parse_args()

glacier = boto3.client('glacier')
glacier_backup_engine = GlacierBackupEngine(glacier, args.vault_name, args.glacier_metadata_file_path)

try:
	glacier_backup_engine.backup_directory(args.directory)

	for entry in scantree__(args.directory):
		print(entry)
		if entry.is_dir():
			glacier_backup_engine.backup_directory(entry.path)
finally:
	glacier_backup_engine.save_glacier_metadata()
