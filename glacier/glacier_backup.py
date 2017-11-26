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

def backup_directory_to_glacier(glacier, vault_name, directory, glacier_metadata):

	files_to_archive = [entry for entry in os.scandir(directory) if entry.is_file()]

	archive_path = create_archive(files_to_archive)
	archive_sum = sha256sum(archive_path)
	archive_description = str(base64.b64encode(directory.encode('ascii')))

	if not directory in glacier_metadata:
		msg = "Detected no metadata for the archive for {0} - Uploading the archive to glacier".format(directory)
		upload_archive = True
	elif archive_sum != glacier_metadata[directory]['sha256sum']:
		msg = "Detected new sha256 sum of the archive for {0} - Uploading the new archive to glacier, and deleting the old one.".format(directory)
		upload_archive = True
	else:
		msg = "Sha256sum of the archive for {0} has not changed, so it will not be uploaded".format(directory)
		upload_archive = False

	print(msg)

	if upload_archive:
		glacier_upload =  glacier_uploader.upload(glacier, vault_name, archive_path, archive_description)
		glacier_metadata[directory] = {
			'archive_id' : glacier_upload['archiveId'],
			'sha256sum' : archive_sum
		}


parser = argparse.ArgumentParser(description="Backup a directory to AWS glacier")
parser.add_argument('vault_name', metavar="vault name", type=str, help="name of the glacier vault to back up data to")
parser.add_argument('directory', metavar='directory', type=str, help="absolute path of the directory to back up")
parser.add_argument('--glacier_metadata_file_path', metavar='glacier metadata file', type=str, help='(optional) path to a file to read and store glacier archive metadata')
args = parser.parse_args()

glacier = boto3.client('glacier')

glacier_metadata = {}

if args.glacier_metadata_file_path and os.path.exists(args.glacier_metadata_file_path):
	with open(args.glacier_metadata_file_path, 'rb') as glacier_metadata_file:
		glacier_metadata = pickle.load(glacier_metadata_file)

try:
	backup_directory_to_glacier(glacier, args.vault_name, args.directory, glacier_metadata)

	for entry in scantree__(args.directory):
		print(entry)
		if entry.is_dir():
			backup_directory_to_glacier(glacier, args.vault_name, entry.path, glacier_metadata)
finally:
	with open(args.glacier_metadata_file_path, 'wb') as glacier_metadata_file:
		pickle.dump(glacier_metadata, glacier_metadata_file)


#if entry found, compute its sha256 hash. If different, delete the old archive, and upload the new one. Otherwise, do nothing

#before we upload.. need to check if we should.