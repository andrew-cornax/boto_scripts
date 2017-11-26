import os
import argparse
import boto3
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
	archive = tarfile.open("/tmp/glacier_{0}.tar.bz2".format(time.strftime("%s")), "w:bz2")
	for file in file_list:
		archive.add(file.path)
	return archive

def sha256sum(file_path):
	s = hashlib.sha256()
	with open(file_path, "rb") as file:
		for block in iter(lambda : file.read(65536), b''):
			s.update(block)
	return s.hexdigest()

parser = argparse.ArgumentParser(description="Backup a directory to AWS glacier")
parser.add_argument('vault_name', metavar="vault name", type=str, help="name of the glacier vault to back up data to")
parser.add_argument('directory', metavar='directory', type=str, help="absolute path of the directory to back up")
parser.add_argument('--glacier_metadata_file' metavar='glacier metadata file', type=str, help='(optional) path to a file to read and store glacier archive metadata')
args = parser.parse_args()

glacier = boto3.client('glacier')

glacier_metadata = {}

if args.glacier_metadata_file:
	glacier_metadata = pickle.load(args.glacier_metadata_file)

try:
	for entry in scantree__(args.directory):
		if entry.is_dir():
			files_to_archive = [subentry for subentry in os.scandir(entry) if not subentry.is_dir()]
			archive = create_archive(files_to_archive)
			archive_sum = sha256sum(archive)

			if not entry.path in glacier_metadata:
				msg = "Detected no metadata for the archive for {0} - Uploading the archive to glacier".format(entry.path)
				upload_archive = True
			elif archive_sum != glacier_metadata[entry.path]['sha256sum']:
				msg = "Detected new sha256 sum of the archive for {0} - Uploading the new archive to glacier, and deleting the old one.".format(entry.path)
				upload_archive = True
			else:
				msg = "Sha256sum of the archive for {0} has not changed, so it will not be uploaded"

			print(msg)

			if upload_archive:
				archive_id = glacier_uploader.upload(glacier, args.vault_name, archive, archive_sum)
				glacier_metadata[entry.path] = {
					'archive_id' : archive_id,
					'sha256sum' : archive_sum
				}

finally:
	pickle.dump(glacier_metadata, glacier_metadata_file)


#if entry found, compute its sha256 hash. If different, delete the old archive, and upload the new one. Otherwise, do nothing

#before we upload.. need to check if we should.