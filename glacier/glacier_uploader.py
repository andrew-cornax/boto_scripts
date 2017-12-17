import boto3
from utils import *
import os
import sys
import binascii

#MULTIPART_BYTE_THRESHOLD = 4294967296


MULTIPART_BYTE_THRESHOLD = 0


def upload(glacier, vault_name, archive_path, archive_description):

	archive_size = os.path.getsize(archive_path)

	if archive_size < MULTIPART_BYTE_THRESHOLD:
		with open(archive_path, 'rb') as archive:
			return glacier.upload_archive(
			    vaultName=vault_name,
			    archiveDescription=archive_description,
			    body=archive
			)

	else:
		bytes_per_part = 1048576
		with open(archive_path, 'rb') as archive:

			multipart_upload = glacier.initiate_multipart_upload(
				vaultName=vault_name,
				archiveDescription=archive_description,
				partSize=str(bytes_per_part))

			print(multipart_upload)

			seek_pos=0

			checksums = []

			while (seek_pos < archive_size):
				archive.seek(seek_pos)
				range_start = seek_pos

				next_seek_pos = seek_pos+bytes_per_part
				if archive_size < next_seek_pos -1:
					range_end = archive_size-1
				else:
					range_end = next_seek_pos-1

				part_bytes = archive.read(bytes_per_part)
				checksums += calculate_1MB_checksums(part_bytes)

				response = glacier.upload_multipart_part(
					vaultName=vault_name,
					uploadId=multipart_upload["uploadId"],
					body=part_bytes,
					range="bytes {0}-{1}/*".format(range_start, range_end))

				print(response)

				seek_pos += bytes_per_part

			done = glacier.complete_multipart_upload(
				vaultName=vault_name,
				uploadId=multipart_upload["uploadId"],
				archiveSize=str(archive_size),
				checksum=binascii.hexlify(calculate_sha256_tree_hash(checksums)[0]).decode('utf-8'))
			print(done)


upload(boto3.client('glacier'), "desktop_backups", sys.argv[1], "test archive")



# http://boto3.readthedocs.io/en/latest/reference/services/glacier.html#Glacier.Client.initiate_multipart_upload
# http://boto3.readthedocs.io/en/latest/reference/services/glacier.html#Glacier.Vault.upload_archive
# http://boto3.readthedocs.io/en/latest/reference/services/glacier.html#Glacier.Client.upload_multipart_part