import argparse
import base64
import boto3
import glacier_uploader
import json
import os
import tarfile
import traceback
import time
from utils import sha256sum


class GlacierBackupEngine:

    def __init__(self, glacier_client, vault_name, root_directory, glacier_metadata_file_path):
        self.glacier = glacier_client
        self.vault_name = vault_name
        self.root_directory = root_directory
        self.glacier_metadata_file_path = glacier_metadata_file_path
        self.__load_glacier_metadata()

    def __scan_tree(self, directory):
        scan = os.scandir(directory)
        for entry in scan:
            if entry.is_dir():
                yield entry
                yield from self.__scan_tree(entry.path)
            else:
                yield entry

    @staticmethod
    def __create_archive(file_list):
        archive_path = "/tmp/glacier_{0}.tar.bz2".format(time.strftime("%s"))
        with tarfile.open(archive_path, "w:bz2") as archive:
            for file in file_list:
                archive.add(file.path)
        return archive_path

    def __load_glacier_metadata(self):
        if self.glacier_metadata_file_path and os.path.exists(self.glacier_metadata_file_path) and os.path.getsize(self.glacier_metadata_file_path) > 0:
            with open(self.glacier_metadata_file_path, 'r') as glacier_metadata_file:
                self.glacier_metadata = json.load(glacier_metadata_file)
        else:
            self.glacier_metadata = {}

    def __save_glacier_metadata(self):
        with open(self.glacier_metadata_file_path, 'w') as glacier_metadata_file:
            json.dump(self.glacier_metadata, glacier_metadata_file)

    def __backup_directory(self, directory):
        """Upload the contents of a given directory to glacier as a single tar.bz2 archive. Note that only files are archived - subdirectories are ignored."""
        files_to_archive = [entry for entry in os.scandir(directory) if entry.is_file()]

        archive_path = self.__create_archive(files_to_archive)
        archive_sum = sha256sum(archive_path)
        archive_description = str(base64.b64encode(directory.encode('ascii')))

        if directory not in self.glacier_metadata:
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
            try:
                glacier_upload = glacier_uploader.upload(self.glacier, self.vault_name, archive_path, archive_description)
                self.glacier_metadata[directory] = {
                    'archive_id': glacier_upload['archiveId'],
                    'sha256sum': archive_sum
                }
                print("Upload of {0} was successful. Now saving metadata and cleaning up its associated archive {1}".format(directory, archive_path))
                self.__save_glacier_metadata()
                os.remove(archive_path)
            except Exception as e:
                print("{0}: Could not upload archive {1} to glacier".format(type(e).__name__, archive_path))
                print("StackTrace: {0}".format(traceback.print_exc()))

    def start_backup(self):
        self.__backup_directory(self.root_directory)

        for entry in self.__scan_tree(self.root_directory):
            if entry.is_dir():
                self.__backup_directory(entry.path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backup a directory (and its contents) to AWS glacier")
    parser.add_argument('--vault_name', metavar="vault name", type=str, required=True, help="name of the glacier vault to back up data to")
    parser.add_argument('--directory', metavar='directory', type=str, required=True, help="absolute path of the directory to back up")
    parser.add_argument('--glacier_metadata_file_path', metavar='glacier metadata file', type=str, default="/tmp/glacier_metadata.json", help='(optional) path to a file to read and store glacier archive json metadata')
    args = parser.parse_args()

    glacier = boto3.client('glacier')
    glacier_backup_engine = GlacierBackupEngine(glacier, args.vault_name, args.directory, args.glacier_metadata_file_path)
    glacier_backup_engine.start_backup()
