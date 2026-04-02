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
        self.directories = set()
        self.storage_servers = [None]

        self.playback()
    
    # to-do: checkpoint and log system
    def playback(self):
        self.data_dir = f'{self.project_name}_name_server/data'
        self.checkpoint = f'{self.server_name}_name_server/table.ckpt'
        self.log = f'{self.server_name}_name_server/table.txn'
        self.log_entries = 0
    
    def compact(self):
        pass
    
    def clean_orphans(self):
        # check for files on disk that are no longer in self.files and self.directories
        pass
    
    def ls(self, client_path):
        client_path = f'{client_path}/'
        directory_files = set()

        for path in self.files.keys():
            if path.startswith(client_path):
                directory_files.add(path.partition(client_path)[2].partition('/')[0])
        
        for path in self.directories:
            if path.startswith(client_path):
                directory_files.add(path.partition(client_path)[2].partition('/')[0])
        
        return list(directory_files)

    def cd(self, client_path, dest_dir):
        dest_path = f'{client_path}/{dest_dir}'
        return dest_path in self.directories

    def create(self, client_path, filename):
        path = f'{client_path}/{filename}'

        if path not in self.files and path not in self.directories:
            file_id = uuid.uuid4().hex
            storage_server = hash(path) % len(self.storage_servers)
            self.files[path] = FileData(file_id, 0, 0, storage_server)

    def remove(self, client_path, filename):
        path = f'{client_path}/{filename}'
        if path in self.files:
            self.files.pop(path)
            # tell storage servers to remove
            # to-do

    def mkdir(self, client_path, dirname):
        dirpath = f'{client_path}/{dirname}'

        if dirpath not in self.files and dirpath not in self.directories:
            self.directories.add(dirpath)
    
    def rmdir(self, client_path, dirname):
        dirpath = f'{client_path}/{dirname}'

        if dirpath in self.directories:
            self.directories.remove(dirpath)

            # remove all files in that directory
            for path in self.files.keys():
                if path.startswith(dirpath):
                    self.files.pop(path)
        
            for path in self.directories:
                if path.startswith(dirpath):
                    self.directories.remove(path)
    