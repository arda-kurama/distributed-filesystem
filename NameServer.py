import uuid
import hashlib

class FileData:
    def __init__(self, id, size, checksum, server_num):
        self.id = id
        self.size = size
        self.checksum = checksum
        self.storage_server = server_num

class NameServer():
    def __init__(self, project_name):
        self.project_name = project_name
        self.files = dict() # path -> FileData()
        self.storage_servers = [None]
    
    def ls(self, client_path):
        directory_files = set()
        client_path = f'{client_path}/'
        for path in self.files.keys():
            if path.startswith(client_path):
                directory_files.add(path.partition(client_path)[2].partition('/')[0])
        return list(directory_files)

    def cd(self, client_path, dest_dir):
        # to-do
        pass

    def create(self, client_path, filename):
        # check if file already exists
        path = f'{client_path}/{filename}'
        if path in self.files:
            return False
        
        # create unique file_id
        file_id = uuid.uuid4().hex

        # assign storage server based on file_id
        storage_server = hash(path) % len(self.storage_servers)
        
        # store metadata in hash table
        self.files[path] = FileData(file_id, 0, 0, storage_server)

        return True

    def mkdir(self, client_path, dirname):
        # to-do
        pass

    def remove(self, client_path, filename):
        path = f'{client_path}/{filename}'
        if path in self.files:
            dict().pop(path)
            # tell storage servers to remove
            return True
        return False