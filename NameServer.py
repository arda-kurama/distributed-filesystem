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
HEARTBEAT_TIMEOUT = 60

# metadata for files stored in NameServer paths
class FileData:
    def __init__(self, id, path, owner, servers, permissions, lock_owner=None, lock_expire=None):
        self.id = id
        self.path = path
        self.owner = owner # username of who created file
        self.servers = servers # list of storage server id's
        self.permissions = permissions # who is allowed to edit ('owner', 'all')
        self.lock_owner = lock_owner # who is currently editing
        self.lock_expire = lock_expire

# metadata for storage servers registered in NameServer storage_servers
class StorageServerInfo:
    def __init__(self, id, host, port, files, last_heartbeat=0, alive=False):
        self.id = id
        self.host = host
        self.port = port
        self.files = files # list of file id's it has stored
        self.last_heartbeat = last_heartbeat
        self.alive = alive

class NameServer():
    def __init__(self, project_name):
        self.project_name = project_name
        self.server_name = f'{self.project_name}-NS'
        self.paths = dict() # path -> FileData()
        self.dir_tree = defaultdict(lambda: {
            'dirs': set(),
            'files': set(),
        })
        # example structure:
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

        self.storage_servers = {} # server_id -> StorageServerInfo

        # initialize files and directories using checkpoint and log files
        self.playback()
        self.next_storage_server_id = max(info.id for info in self.storage_servers.values()) + 1 if self.storage_servers else 1
        print(f'next_id: {self.next_storage_server_id}')

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
        self.checkpoint = f'{self.server_name}/table.ckpt'
        self.log = f'{self.server_name}/table.txn'
        self.log_entries = 0

        # fresh start
        if not os.path.exists(f'{self.server_name}'):
            os.mkdir(f'{self.server_name}')

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
                for path, meta in ckpt_paths.items():
                    self.paths[path] = FileData(
                        id=meta['id'],
                        path=meta['path'],
                        owner=meta['owner'],
                        servers=meta['servers'],
                        permissions=meta['permissions']
                    )
                ckpt_dir_tree = ckpt['dir_tree']
                for dir, entry in ckpt_dir_tree.items():
                    self.dir_tree[dir] = {
                        'dirs': set(entry.get('dirs', [])),
                        'files': set(entry.get('files', []))
                    }
                ckpt_storage_servers = ckpt['servers']
                for id, entry in ckpt_storage_servers.items():
                    self.storage_servers[id] = StorageServerInfo(
                        id=entry['id'],
                        host=entry['host'],
                        port=entry['port'],
                        files=entry['files']
                    )
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

                    if operation['operation'] == 'create':
                        parent_dir, _, name = operation['path'].rpartition('/')
                        if operation['type'] == 'file':
                            self.paths[operation['path']] = FileData(
                                id=operation['id'],
                                path=operation['path'],
                                owner=operation['owner'],
                                servers=operation['servers'],
                                permissions=operation['permissions']
                            )
                            self.dir_tree[parent_dir]['files'].add(name)
                        else:
                            self.dir_tree[operation['path']]
                            self.dir_tree[parent_dir]['dirs'].add(name)
                    if operation['operation'] == 'remove':
                        parent_dir, _, name = operation['path'].rpartition('/')
                        if operation['type'] == 'file':
                            self.paths.pop(operation['path'])
                            self.dir_tree[parent_dir]['files'].remove(name)
                        else:
                            self.dir_tree.pop(operation['path'])
                            self.dir_tree[parent_dir]['dirs'].remove(name)
                    if operation['operation'] == 'register':
                        self.storage_servers[operation['id']] = StorageServerInfo(
                            id=operation['id'],
                            host=operation['host'],
                            port=operation['port'],
                            files=operation['files']
                        )
                    
        except json.JSONDecodeError:
            # empty log file
            pass
    
    def compact(self):
        checkpoint_data = {
            'paths': {},
            'dir_tree': {},
            'servers': {}
        }

        for path, meta in self.paths.items():
            checkpoint_data['paths'][path] = {
                'id': meta.id,
                'path': meta.path,
                'owner': meta.owner,
                'servers': meta.servers,
                'permissions': meta.permissions
            }
        
        for dir, entry in self.dir_tree.items():
            checkpoint_data['dir_tree'][dir] = {
                'dirs': sorted(entry['dirs']),
                'files': sorted(entry['files'])
            }
        
        for id, entry in self.storage_servers.items():
            checkpoint_data['servers'][id] = {
                'id': entry.id,
                'host': entry.host,
                'port': entry.port,
                'files': entry.files
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
            # print(fd, event)
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
            return

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
                # self.print_tree()
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

    # general rpc handler
    def response(self, message):
        rpc = json.loads(message.decode('utf-8'))

        # check if message is correctly formatted
        if 'method' not in rpc:
            return self.make_reply('invalid message, no method provided')
        
        # client methods
        if rpc['method'] == 'ls':
            return self.ls(rpc.get('path'))
        if rpc['method'] == 'cd':
            return self.cd(rpc.get('path'))
        if rpc['method'] == 'create':
            return self.create(rpc.get('path'), rpc.get('filename'))
        if rpc['method'] == 'remove':
            return self.remove(rpc.get('path'), rpc.get('filename'))
        if rpc['method'] == 'mkdir':
            return self.mkdir(rpc.get('path'), rpc.get('dirname'))
        if rpc['method'] == 'rmdir':
            return self.rmdir(rpc.get('path'), rpc.get('dirname'))
        if rpc['method'] == 'open':
            return self.open(rpc.get('path'))
        
        # storage server methods
        if rpc['method'] == 'register':
            return self.register_storage_server(rpc.get('id'), rpc.get('host'), rpc.get('port'))
        if rpc['method'] == 'heartbeat':
            info = self.storage_servers[rpc.get('id')]
            info.last_heartbeat = time.time()
            info.alive = True

        # more methods to test out server functionality
        if rpc['method'] == 'compact':
            self.compact()
            return self.make_reply('success')
        if rpc['method'] == 'clean':
            self.clean_orphans()
            return self.make_reply('success')
        if rpc['method'] == 'tree':
            self.print_tree()
            return self.make_reply('success')
        if rpc['method'] == 'servers':
            print(self.storage_servers)
            return self.make_reply('success')
        
        # method not found
        return self.make_reply('invalid method')

    # individual rpc stubs
    def ls(self, client_path):
        # check that parameters exist
        if client_path is None:
            return self.make_reply('invalid arguments for ls')

        dirs = sorted(self.dir_tree[client_path]['dirs'])
        files = sorted(self.dir_tree[client_path]['files'])
        
        # return reply
        return self.make_reply('success', [dirs, files])

    def cd(self, dest_dir):
        if dest_dir is None:
            return self.make_reply('invalid arguments for cd')

        reply = {'result': 'success' if dest_dir in self.dir_tree else 'failure', 'return': dest_dir}
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

        storage_server = self.choose_storage_server(path)
        if storage_server is None:
            return self.make_reply('no storage servers available')
        
        reply = self.rpc_storage_server(storage_server, {
            'method': 'create',
            'id': file_id,
            'path': path,
        })

        if reply['result'] != 'success':
            return self.make_reply(f"storage create failed: {reply['result']}")
        
        # 1. update log file
        operation = {'operation': 'create',
                        'type': 'file',
                        'id': file_id,
                        'path': path,
                        'owner': None,
                        'servers': [storage_server],
                        'permissions': 'owner'}
        with open(self.log, 'a') as f:
            f.write(json.dumps(operation) + '\n')

            f.flush()
            os.fsync(f.fileno())
        self.log_entries += 1
        
        # 2. add metadata to hash table in memory
        self.paths[path] = FileData(file_id, path, None, [storage_server], 'owner')
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

    def open(self, path):
        if path is None:
            return self.make_reply('invalid arguments for open')

        if path not in self.paths:
            return self.make_reply('file not found')

        meta = self.paths[path]
        servers = meta.servers
        server_id = servers[0]
        if server_id not in self.storage_servers:
            return self.make_reply('storage server unavailable')

        ss = self.storage_servers[server_id]
        return self.make_reply('success', {
            'file_id': meta.id,
            'storage_server_id': ss.id,
            'host': ss.host,
            'port': ss.port
        })

    def child_path(self, parent_dir, name):
        return f'{parent_dir}/{name}'

    def print_tree(self):
        print(json.dumps(self.dir_tree, indent=4, sort_keys=True, default=str))
    
    def make_reply(self, result, value=None):
        return json.dumps({'result': result, 'return': value}).encode('utf-8')

    # storage server methods
    def register_storage_server(self, server_id, host, port):
        if host is None or port is None:
            return self.make_reply('invalid arguments for register')
        
        if server_id is None:
            server_id = self.next_storage_server_id
            self.next_storage_server_id += 1
        
            # write to log file
            operation = {'operation': 'register',
                        'id': server_id,
                        'host': host,
                        'port': port,
                        'files': []}
            with open(self.log, 'a') as f:
                f.write(json.dumps(operation) + '\n')
                f.flush()
                os.fsync(f.fileno())
            self.log_entries += 1

            self.storage_servers[server_id] = StorageServerInfo(
                id=server_id,
                host=host,
                port=port,
                last_heartbeat=time.time(),
                alive=True,
                files=[]
            )

            print(f'registered storage server {server_id} at {host}:{port}')
        else:
            info = self.storage_servers[server_id]
            info.last_heartbeat = time.time()
            info.alive = True

        return self.make_reply('success', server_id)

    def rpc_storage_server(self, server_id, message):
        if server_id not in self.storage_servers:
            raise RuntimeError(f'unknown storage server {server_id}')

        info = self.storage_servers[server_id]

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect((info.host, info.port))

        try:
            message_bytes = json.dumps(message).encode('utf-8')
            header = struct.pack('!I', len(message_bytes))
            sock.sendall(header + message_bytes)

            header = self.recv_exact(sock, HEADER_LEN)
            length = struct.unpack('!I', header)[0]
            reply_bytes = self.recv_exact(sock, length)
            return json.loads(reply_bytes.decode('utf-8'))
        finally:
            sock.close()

    def recv_exact(self, sock, n):
        chunks = []
        got = 0
        while got < n:
            chunk = sock.recv(n - got)
            if chunk == b'':
                raise RuntimeError('socket closed')
            chunks.append(chunk)
            got += len(chunk)
        return b''.join(chunks)

    def choose_storage_server(self, path):
        if not self.storage_servers:
            return None

        live_ids = sorted(id for id, info in self.storage_servers.items() if info.alive)
        if not live_ids:
            return None
        return live_ids[hash(path) % len(live_ids)]

    def reap_storage_servers(self):
        now = time.time()
        for info in self.storage_servers.values():
            if now - info.last_heartbeat > HEARTBEAT_TIMEOUT:
                info.alive = False

    def run(self):
        threading.Thread(target=self.update_register, daemon=True).start()

        try:
            while True:
                # check if storage servers are still up
                self.reap_storage_servers()

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
