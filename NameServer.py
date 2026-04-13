import socket
import sys
import struct
import json
import os
import uuid
import threading
import time
import select
import signal
import hashlib
from collections import defaultdict

BUFSIZE = 4096
HEADER_LEN = 4
REGISTER_UPDATE_PERIOD = 60

class FileData:
    def __init__(self, id, size, checksum, server_num):
        self.id = id
        self.size = size
        self.checksum = checksum
        self.storage_server = server_num

class NameServer():
    def __init__(self, project_name):
        self.project_name = project_name
        # to-do: update files and directories data structures to speed up rpc's
        self.paths = dict() # path -> FileData()
        self.dir_tree = defaultdict(lambda: {
            'dirs': set(),
            'files': set(),
        })
        # structure:
        # dir_tree = {
        #     '/': {
        #         'dirs': {'docs', 'tmp'},
        #         'files': set(),
        #     },
        #     '/docs': {
        #         'dirs': set(),
        #         'files': {'report.txt', 'notes.txt'},
        #     },
        #     '/tmp': {
        #         'dirs': set(),
        #         'files': set(),
        #     },
        # }
        self.storage_servers = [None]

        # initialize files and directories using checkpoint and log files
        self.playback()

        # create socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.hostname = socket.gethostname()
        self.server_socket.bind((self.hostname, 0))
        self.server_socket.listen()
        self.server_socket.setblocking(False)
        _, self.port = self.server_socket.getsockname()

        print(f'Listening on port {self.port}')

        # register on name server
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.register()

        # set up polling for event driven
        self.epoll = select.epoll()
        self.epoll.register(self.server_socket.fileno(), select.EPOLLIN)
        self.client_sockets = dict()
        self.client_read_buffers = dict()
        self.client_write_buffers = dict()
        self.client_msglens = dict()
        self.client_bytes = dict()
    
    def playback(self):
        self.checkpoint = f'{self.project_name}/table.ckpt'
        self.log = f'{self.project_name}/table.txn'
        self.log_entries = 0

        # fresh start
        if not os.path.exists(f'{self.project_name}'):
            os.mkdir(f'{self.project_name}')

            if not os.path.exists(self.log) or not os.path.exists(self.checkpoint):
                # create relevant files if they dont exist
                if not os.path.exists(self.checkpoint):
                    with open(self.checkpoint, 'w') as _:
                        pass
                if not os.path.exists(self.log):
                    with open(self.log, 'w') as _:
                        pass
            return
        
        # load checkpoint into memory
        try:
            with open(self.checkpoint, 'r') as f:
                ckpt = json.load(f)
                ckpt_paths = ckpt['paths']
                ckpt_dir_tree = ckpt['dir_tree']
                for path, meta in ckpt_paths.items():
                    self.paths[path] = FileData(
                        id=meta['id'],
                        size=meta['size'],
                        checksum=meta['checksum'],
                        server_num=meta['storage_server']
                    )
                for dir, entry in ckpt_dir_tree.items():
                    self.dir_tree[dir] = {
                        'dirs': set(entry.get('dirs', [])),
                        'files': set(entry.get('files', []))
                    }
        except json.JSONDecodeError:
            # empty checkpoint file
            pass
        
        # replay log entries in order
        try:
            with open(self.log, 'r') as f:
                for line in f:
                    # skip empty lines
                    if not line.strip():
                        continue

                    operation = json.loads(line)
                    self.log_entries += 1

                    parent_dir, _, name = operation['path'].rpartition('/')
                    if operation['operation'] == 'create':
                        if operation['type'] == 'file':
                            self.paths[operation['path']] = FileData(
                                id=operation['id'],
                                size=operation['size'],
                                checksum=operation['checksum'],
                                server_num=operation['storage_server']
                            )
                            self.dir_tree[parent_dir]['files'].add(name)
                        else:
                            self.dir_tree[operation['path']]
                            self.dir_tree[parent_dir]['dirs'].add(name)
                    if operation['operation'] == 'remove':
                        if operation['type'] == 'file':
                            self.paths.pop(operation['path'])
                            self.dir_tree[parent_dir]['files'].remove(name)
                        else:
                            self.dir_tree.pop(operation['path'])
                            self.dir_tree[parent_dir]['dirs'].remove(name)
                    
        except json.JSONDecodeError:
            # empty log file
            pass
    
    def compact(self):
        checkpoint_data = {
            'paths': {},
            'dir_tree': {},
        }

        for path, meta in self.paths.items():
            checkpoint_data['paths'][path] = {
                'id': meta.id,
                'size': meta.size,
                'checksum': meta.checksum,
                'storage_server': meta.storage_server
            }
        
        for dir, entry in self.dir_tree.items():
            checkpoint_data['dir_tree'][dir] = {
                'dirs': sorted(entry['dirs']),
                'files': sorted(entry['files'])
            }

        # write current data to new checkpoint
        tmp = f'{self.checkpoint}.tmp'
        with open(tmp, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        
        # atomically update checkpoint file
        self.log_entries = 0
        os.replace(tmp, self.checkpoint)

        # clear log file
        os.remove(self.log)
        with open(self.log, 'a') as _:
            pass
    
    def register(self):
        message = {
            'type': 'name_server',
            'owner': 'qhynes',
            'port': self.port,
            'project': self.project_name
        }

        # register with name server
        name_server = ('catalog.cse.nd.edu', 9097)
        message_bytes = json.dumps(message).encode('utf-8')
        self.udp_sock.sendto(message_bytes, name_server)
    
    def update_register(self):
        # periodically tell name server information
        while True:
            time.sleep(REGISTER_UPDATE_PERIOD)
            self.register()
    
    def handle_events(self, events):
        for fd, event in events:
            if fd == self.server_socket.fileno(): # new connection
                self.connection_event()
            elif event & select.EPOLLIN: # socket readable
                self.read_event(fd)
            elif event & select.EPOLLOUT: # socket writeable
                self.write_event(fd)
    
    def connection_event(self):
        try:
            client_socket, _ = self.server_socket.accept()
            client_socket.setblocking(False)
        except (BlockingIOError, OSError):
            pass

        fd = client_socket.fileno()
        self.epoll.register(fd, select.EPOLLIN)

        self.client_sockets[fd] = client_socket
        self.client_read_buffers[fd] = bytearray()
        self.client_write_buffers[fd] = bytearray()
        self.client_msglens[fd] = None
        self.client_bytes[fd] = 0
    
    def read_event(self, fd):
        sock = self.client_sockets[fd]

        try:
            data = sock.recv(BUFSIZE)

            if not data:
                raise ConnectionResetError
            
            # update buffer and bytes sent variable
            self.client_read_buffers[fd].extend(data)
            self.client_bytes[fd] += len(data)

            # store message length
            if not self.client_msglens[fd] and self.client_bytes[fd] >= HEADER_LEN:
                header = self.client_read_buffers[fd][:HEADER_LEN]
                message_length = struct.unpack('!I', header)[0]
                self.client_msglens[fd] = message_length
            
            # received entire message or more
            if self.client_bytes[fd] >= HEADER_LEN + self.client_msglens[fd]:
                
                # slice message out of buffer and respond
                message = self.client_read_buffers[fd][HEADER_LEN:HEADER_LEN + self.client_msglens[fd]]
                reply_bytes = self.response(message)
                self.print_tree()
                self.client_write_buffers[fd] = bytearray(reply_bytes)

                # remove message from buffer and reset
                self.client_read_buffers[fd] = self.client_read_buffers[fd][HEADER_LEN + self.client_msglens[fd]:]
                self.client_bytes[fd] = len(self.client_read_buffers[fd])
                self.client_msglens[fd] = None

                # change socket to writeable and reset other information
                self.epoll.modify(fd, select.EPOLLOUT)

        except (ConnectionResetError, BrokenPipeError):
            self.epoll.unregister(fd)

            try:
                sock.close()
            except OSError:
                pass

            del self.client_sockets[fd]
            del self.client_read_buffers[fd]
            del self.client_write_buffers[fd]
            del self.client_msglens[fd]
            del self.client_bytes[fd]
    
    def write_event(self, fd):
        sock = self.client_sockets[fd]
        message = self.client_write_buffers[fd]

        # format message and store length
        if not self.client_msglens[fd]:
            header = struct.pack('!I', len(message))
            message = header + message
            self.client_write_buffers[fd] = bytearray(message)
            self.client_msglens[fd] = len(message)

        bytes_sent = sock.send(message[self.client_bytes[fd]:])
        self.client_bytes[fd] += bytes_sent

        # sent full message
        if self.client_bytes[fd] >= self.client_msglens[fd]:
            self.epoll.modify(fd, select.EPOLLIN)
            self.epoll.unregister(fd)
            sock.close()

            del self.client_sockets[fd]
            del self.client_read_buffers[fd]
            del self.client_write_buffers[fd]
            del self.client_msglens[fd]
            del self.client_bytes[fd]
    
    def close(self):
        self.epoll.close()
        self.server_socket.close()
        self.udp_sock.close()
    
    # don't need? there are no files on disk for the name server
    # def clean_orphans(self):
    #     # get dict of filepaths in hash table
    #     active_files = {meta.path for meta in self.ht.data.values()}

    #     # remove any files on disk that aren't in ht
    #     for name in os.listdir(self.data_dir):
    #         path = os.path.join(self.data_dir, name)
    #         if path not in active_files:
    #             os.remove(path)

    def response(self, message):
        rpc = json.loads(message.decode('utf-8'))

        # check if message is correctly formatted
        if 'method' not in rpc:
            reply = {'result': 'invalid message, no method provided', 'return': None}
            return json.dumps(reply).encode('utf-8')
        
        # to-do: resolve + validate path

        # perform method
        if rpc['method'] == 'ls':
            return self.ls(rpc.get('path'))
        if rpc['method'] == 'cd':
            return self.cd(rpc.get('path'), rpc.get('dest_dir'))
        if rpc['method'] == 'create':
            return self.create(rpc.get('path'), rpc.get('filename'))
        if rpc['method'] == 'remove':
            return self.remove(rpc.get('path'), rpc.get('filename'))
        if rpc['method'] == 'mkdir':
            return self.mkdir(rpc.get('path'), rpc.get('dirname'))
        if rpc['method'] == 'rmdir':
            return self.rmdir(rpc.get('path'), rpc.get('dirname'))

        # more methods to test out server functionality
        if rpc['method'] == 'compact':
            self.compact()
            reply = {'result': 'success', 'return': None}
            return json.dumps(reply).encode('utf-8')
        if rpc['method'] == 'clean':
            self.clean_orphans()
            reply = {'result': 'success', 'return': None}
            return json.dumps(reply).encode('utf-8')
        
        # method not found
        reply = {'result': 'invalid method', 'return': None}
        return json.dumps(reply).encode('utf-8')

    def ls(self, client_path):
        # check that parameters exist
        if client_path is None:
            return self.make_reply('invalid arguments for ls')

        dirs = sorted(self.dir_tree[client_path]['dirs'])
        files = sorted(self.dir_tree[client_path]['files'])
        
        # return reply
        return self.make_reply('success', [dirs, files])

    def cd(self, client_path, dest_dir):
        if client_path is None or dest_dir is None:
            return self.make_reply('invalid arguments for cd')

        dest_path = self.child_path(client_path, dest_dir)
        reply = {'result': 'success' if dest_dir in self.dir_tree[client_path]['dirs'] else 'failure', 'return': dest_path}
        return json.dumps(reply).encode('utf-8')

    def create(self, client_path, filename):
        if client_path is None or filename is None:
            return self.make_reply('invalid arguments for create')

        if filename in self.dir_tree[client_path]['dirs']:
            return self.make_reply('file name already taken by directory')
        
        if filename in self.dir_tree[client_path]['files']:
            return self.make_reply('success')

        # crash safe ordering
        path = self.child_path(client_path, filename)
        file_id = uuid.uuid4().hex
        storage_server = hash(path) % len(self.storage_servers)
        
        # 1. update log file
        operation = {'operation': 'create',
                        'type': 'file',
                        'id': file_id,
                        'path': path,
                        'size': 0,
                        'checksum': None,
                        'storage_server': storage_server}
        with open(self.log, 'a') as f:
            f.write(json.dumps(operation) + '\n')

            f.flush()
            os.fsync(f.fileno())
        self.log_entries += 1
        
        # 2. add metadata to hash table in memory
        self.paths[path] = FileData(file_id, 0, None, storage_server)
        self.dir_tree[client_path]['files'].add(filename)

        return self.make_reply('success')

    def remove(self, client_path, filename):
        if client_path is None or filename is None:
            return self.make_reply('invalid arguments for remove')
        
        if filename in self.dir_tree[client_path]['dirs']:
            return self.make_reply('cannot use remove on directory, use rmdir instead')
        
        # makes operation idempotent
        if filename not in self.dir_tree[client_path]['files']:
            return self.make_reply('success')

        # tell storage servers to remove
        # to-do
    
        # crash safe ordering
        # 1. update log file
        path = self.child_path(client_path, filename)
        operation = {'operation': 'remove', 'type': 'file', 'path': path}
        with open(self.log, 'a') as f:
            f.write(json.dumps(operation) + '\n')

            f.flush()
            os.fsync(f.fileno())
        self.log_entries += 1
        
        # 2. update file structure in memory
        self.paths.pop(path)
        self.dir_tree[client_path]['files'].remove(filename)
        
        return self.make_reply('success')

    def mkdir(self, client_path, dirname):
        if client_path is None or dirname is None:
            return self.make_reply('invalid arguments for mkdir')

        if dirname in self.dir_tree[client_path]['files']:
            return self.make_reply('directory name already taken by file')

        if dirname in self.dir_tree[client_path]['dirs']:
            return self.make_reply('success')

        dirpath = self.child_path(client_path, dirname)
        operation = {'operation': 'create', 'type': 'directory', 'path': dirpath}
        with open(self.log, 'a') as f:
            f.write(json.dumps(operation) + '\n')

            f.flush()
            os.fsync(f.fileno())
        self.log_entries += 1

        self.dir_tree[dirpath]
        self.dir_tree[client_path]['dirs'].add(dirname)
        
        return self.make_reply('success')
    
    def rmdir(self, client_path, dirname):
        if client_path is None or dirname is None:
            return self.make_reply('invalid arguments for rmdir')

        if dirname in self.dir_tree[client_path]['files']:
            return self.make_reply('cannot use rmdir on file, use remove instead')
        
        if dirname not in self.dir_tree[client_path]['dirs']:
            return self.make_reply('success')
        
        # postorder dfs adds children, then parents so we remove in safe order
        recursive_files = []
        recursive_dirs = []
        def walk(current_dir):
            for file in self.dir_tree[current_dir]['files']:
                recursive_files.append(self.child_path(current_dir, file))
            
            for dirname in self.dir_tree[current_dir]['dirs']:
                dirpath = self.child_path(current_dir, dirname)
                walk(dirpath)
            
            recursive_dirs.append(current_dir)
        
        dirpath = self.child_path(client_path, dirname)
        walk(dirpath)
        
        # log all removes
        with open(self.log, 'a') as f:
            for path in recursive_files:
                operation = {'operation': 'remove', 'type': 'file', 'path': path}
                f.write(json.dumps(operation) + '\n')
            for path in recursive_dirs:
                operation = {'operation': 'remove', 'type': 'directory', 'path': path}
                f.write(json.dumps(operation) + '\n')

            f.flush()
            os.fsync(f.fileno())
        self.log_entries += len(recursive_files) + len(recursive_dirs)

        print(recursive_files)
        print(recursive_dirs)

        # return self.make_reply('success')
        
        # remove from memory
        for path in recursive_files:
            parent_dir, _, filename = path.rpartition('/')
            self.paths.pop(path)
            self.dir_tree[parent_dir]['files'].remove(filename)
        for path in recursive_dirs:
            parent_dir, _, child_dir = path.rpartition('/')
            self.dir_tree.pop(path)
            self.dir_tree[parent_dir]['dirs'].remove(child_dir)
        
        return self.make_reply('success')

    def child_path(self, parent_dir, name):
        return f'{parent_dir}/{name}'

    def resolve_path(self, input_path):
        if input_path.startswith('/'): # absolute path
            pass
        pass

    def print_tree(self):
        print(json.dumps(self.dir_tree, indent=4, sort_keys=True, default=str))
    
    def make_reply(self, result, value=None):
        return json.dumps({'result': result, 'return': value}).encode('utf-8')

    def run(self):
        threading.Thread(target=self.update_register, daemon=True).start()

        try:
            while True:

                # update checkpoint file if needed
                if self.log_entries > 100:
                    self.compact()

                # poll and handle events
                events = self.epoll.poll(timeout=1)
                self.handle_events(events)
                
        finally:
            self.close()
    
def main():
    if len(sys.argv) != 2:
        raise RuntimeError('Usage: python NameServer.py [project_name]')

    server = NameServer(sys.argv[1])
    server.verbose = True

    server.run()

if __name__ == '__main__':
    main()
