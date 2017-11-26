import os
import argparse
import boto3
import glacier_uploader


def scantree__(root_directory):
	scan = os.scandir(root_directory)

	for entry in scan:
		if entry.is_dir():
			yield from scantree__(entry.path)
		else:
			yield entry

parser = argparse.ArgumentParser(description="Backup a directory to AWS glacier")
parser.add_argument('vault_name', metavar="vault name", type=str, help="name of the glacier vault to back up data to")
parser.add_argument('directory', metavar='directory', type=str, help="absolute path of the directory to back up")
args = parser.parse_args()

glacier = boto3.client('glacier')

for entry in scantree__(args.directory):
	if entry.is_dir():
		entries_to_archive = [entry for entry in os.scandir(entry) if not entry.is_dir()]
		archive = create_archive(entries_to_archive)
		glacier_uploader.upload(glacier, args.vault_name, archive)