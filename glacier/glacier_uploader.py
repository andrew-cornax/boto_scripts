import boto3
from botocore.utils import calculate_tree_hash
import os

MULTIPART_BYTE_THRESHOLD = 4294967296


def upload(glacier, vault_name, archive_path, archive_description):

	archive_size = os.path.getsize(archive_path)

	if archive_size < MULTIPART_BYTE_THRESHOLD:
		with open(archive_path, 'rb') as archive:
			return glacier.upload_archive(
			    vaultName=vault_name,
			    archiveDescription=archive_description,
			    body=archive
			)


# http://boto3.readthedocs.io/en/latest/reference/services/glacier.html#Glacier.Client.initiate_multipart_upload
# http://boto3.readthedocs.io/en/latest/reference/services/glacier.html#Glacier.Vault.upload_archive
# http://boto3.readthedocs.io/en/latest/reference/services/glacier.html#Glacier.Client.upload_multipart_part