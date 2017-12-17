import hashlib
import sys
from botocore.utils import calculate_tree_hash
import binascii

def sha256sum(file_path):
	s = hashlib.sha256()
	with open(file_path, "rb") as file:
		for block in iter(lambda : file.read(1024*1024), b''):
			s.update(block)

	return s.hexdigest()


def sha256_part_hashes(file_path):
	hashes = []
	with open(file_path, "rb") as file:
		for block in iter(lambda : file.read(1024*1024), b''):
			s = hashlib.sha256()
			s.update(block)
			hashes.append(s.digest())

	print(hashes)
	return hashes

def calculate_1MB_checksums(raw_bytes):
	hashes = []
	mb = 1024*1024
	for i in range(0, len(raw_bytes)-1, mb):
		print(i)
		s = hashlib.sha256()
		s.update(raw_bytes[i:i+mb])
		hashes.append(s.digest())

	return hashes

def calculate_sha256_tree_hash(hash_list):
	#print(hash_list)
	if len(hash_list) == 1:
		return hash_list
	else:
		parent_hashes = []

		for i in range(0, len(hash_list), 2):
			
			if i == len(hash_list) -1:
				parent_hashes.append(hash_list[i])
			else:
				s = hashlib.sha256()
				s.update(hash_list[i])
				s.update(hash_list[i+1])
				parent_hashes.append(s.digest())

		return calculate_sha256_tree_hash(parent_hashes)


# with open(sys.argv[1], 'rb') as testfile:
# 	sums = calculate_1MB_checksums(testfile.read())
# 	print("New Method")
# 	print(sums)
# 	tree_hash = calculate_sha256_tree_hash(sums)
# 	print(binascii.hexlify(tree_hash[0]))
# 	print(tree_hash)


# print ("Old method")

# sums = sha256_part_hashes(sys.argv[1])
# print(sums)
# digest = calculate_sha256_tree_hash(sums)

# print(str(binascii.hexlify(digest[0])))
# print(str(digest[0]))


# boto_hash = calculate_tree_hash(open(sys.argv[1], "rb"))

# print("boto hash: " + 	boto_hash)
# print(bytes.fromhex(boto_hash))










