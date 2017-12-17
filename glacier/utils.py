import hashlib


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


def calculate_sha256_leaf_hashes(raw_bytes):
    hashes = []
    mb = 1024*1024
    for i in range(0, len(raw_bytes)-1, mb):
        print(i)
        s = hashlib.sha256()
        s.update(raw_bytes[i:i+mb])
        hashes.append(s.digest())

    return hashes


def calculate_sha256_tree_hash(hash_list):
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










