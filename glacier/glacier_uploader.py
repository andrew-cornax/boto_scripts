from utils import *
import os
import binascii

MULTIPART_BYTE_THRESHOLD = 4294967296
BYTES_PER_PART = 1048576


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
        with open(archive_path, 'rb') as archive:

            multipart_upload = glacier.initiate_multipart_upload(
                vaultName=vault_name,
                archiveDescription=archive_description,
                partSize=str(BYTES_PER_PART))

            part_start = 0
            checksums = []
            while part_start < archive_size:

                part_end = part_start+BYTES_PER_PART
                if archive_size < part_end - 1:
                    part_end = archive_size - 1

                part_bytes = archive.read(BYTES_PER_PART)
                checksums += calculate_sha256_leaf_hashes(part_bytes)

                glacier.upload_multipart_part(
                    vaultName=vault_name,
                    uploadId=multipart_upload["uploadId"],
                    body=part_bytes,
                    range="bytes {0}-{1}/*".format(part_start, part_end))

                part_start += BYTES_PER_PART

            glacier.complete_multipart_upload(
                vaultName=vault_name,
                uploadId=multipart_upload["uploadId"],
                archiveSize=str(archive_size),
                checksum=binascii.hexlify(calculate_sha256_tree_hash(checksums)[0]).decode('utf-8'))
