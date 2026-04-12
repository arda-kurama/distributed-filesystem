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
        self.files = dict() # path -> FileData()
        self.directories = set()
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
                ckpt_files = ckpt['files']
                ckpt_dirs = ckpt['directories']
                for path, meta in ckpt_files.items():
                    self.files[path] = FileData(
                        path=meta['path'],
                        size=meta['size'],
                        checksum=meta['checksum']
                    )
                for path in ckpt_dirs:
                    self.directories.add(path)
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

                    if operation['method'] == 'create':
                        self.files[operation['path']] = FileData(
                            path=operation['path'],
                            size=operation['size'],
                            checksum=operation['checksum']
                        )
                    if operation['method'] == 'remove':
                        self.files.pop(operation['path'])
                    if operation['method'] == 'mkdir':
                        pass
                    if operation['method'] == 'rmdir':
                        pass
                    
        except json.JSONDecodeError:
            # empty log file
            pass

        # remove any files on disk that are no longer in hash table
        self.clean_orphans()
    
    def compact(self):
        checkpoint_data = {
            'files': dict(),
            'directories': list(self.directories),
        }

        for path, meta in self.files.items():
            checkpoint_data['files'][path] = {
                'id': meta.id,
                'size': meta.size,
                'checksum': meta.checksum,
                'storage_server': meta.storage_server
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
            reply = {'result': 'Invalid message, no method provided', 'return': None}
            return json.dumps(reply).encode('utf-8')

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
            reply = {'result': 'Success', 'return': None}
            return json.dumps(reply).encode('utf-8')
        if rpc['method'] == 'clean':
            self.clean_orphans()
            reply = {'result': 'Success', 'return': None}
            return json.dumps(reply).encode('utf-8')
        
        # method not found
        reply = {'result': 'Invalid method', 'return': None}
        return json.dumps(reply).encode('utf-8')

    def ls(self, client_path):
        # check that parameters exist
        if client_path is None:
            reply = {'result': 'Invalid arguments for ls', 'return': None}
            return json.dumps(reply).encode('utf-8')

        client_path = f'{client_path}/'
        directory_files = set()

        for path in self.files.keys():
            if path.startswith(client_path):
                directory_files.add(path.partition(client_path)[2].partition('/')[0])
        
        for path in self.directories:
            if path.startswith(client_path):
                directory_files.add(path.partition(client_path)[2].partition('/')[0])
        
        # return reply
        reply = {'result': 'Success', 'return': sorted(list(directory_files))}
        return json.dumps(reply).encode('utf-8')

    def cd(self, client_path, dest_dir):
        if client_path is None or dest_dir is None:
            reply = {'result': 'Invalid arguments for cd', 'return': None}
            return json.dumps(reply).encode('utf-8')

        dest_path = f'{client_path}/{dest_dir}'
        reply = {'result': 'Success' if dest_path in self.directories else 'Failure', 'return': dest_path}
        return json.dumps(reply).encode('utf-8')

    def create(self, client_path, filename):
        if client_path is None or filename is None:
            reply = {'result': 'Invalid arguments for create', 'return': None}
            return json.dumps(reply).encode('utf-8')
        
        path = f'{client_path}/{filename}'

        if path in self.directories:
            reply = {'result': 'File name already taken by directory', 'return': None}
            return json.dumps(reply).encode('utf-8')

        if path not in self.files:
            # crash safe ordering
            file_id = uuid.uuid4().hex
            storage_server = hash(path) % len(self.storage_servers)
            
            # 1. update log file
            operation = {'method': 'create',
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
            self.files[path] = FileData(file_id, 0, None, storage_server)

        # return reply
        reply = {'result': 'Success', 'return': None}
        return json.dumps(reply).encode('utf-8')

    def remove(self, client_path, filename):
        if client_path is None or filename is None:
            reply = {'result': 'Invalid arguments for remove', 'return': None}
            return json.dumps(reply).encode('utf-8')

        path = f'{client_path}/{filename}'
        if path in self.files:
            # tell storage servers to remove
            # to-do
        
            # crash safe ordering
            # 1. update log file
            operation = {'method': 'remove', 'path': path}
            with open(self.log, 'a') as f:
                f.write(json.dumps(operation) + '\n')

                f.flush()
                os.fsync(f.fileno())
            self.log_entries += 1
            
            # 2. update hash table in memory
            self.files.pop(path)
        
        reply = {'result': 'Success', 'return': None}
        return json.dumps(reply).encode('utf-8')

    def mkdir(self, client_path, dirname):
        if client_path is None or dirname is None:
            reply = {'result': 'Invalid arguments for mkdir', 'return': None}
            return json.dumps(reply).encode('utf-8')

        dirpath = f'{client_path}/{dirname}'

        if dirpath in self.files:
            reply = {'result': 'Directory name already taken by file', 'return': None}
            return json.dumps(reply).encode('utf-8')

        if dirpath not in self.directories:
            operation = {'method': 'mkdir', 'path': dirpath}
            with open(self.log, 'a') as f:
                f.write(json.dumps(operation) + '\n')

                f.flush()
                os.fsync(f.fileno())
            self.log_entries += 1

            self.directories.add(dirpath)
        
        reply = {'result': 'Success', 'return': None}
        return json.dumps(reply).encode('utf-8')
    
    def rmdir(self, client_path, dirname):
        if client_path is None or dirname is None:
            reply = {'result': 'Invalid arguments for rmdir', 'return': None}
            return json.dumps(reply).encode('utf-8')

        dirpath = f'{client_path}/{dirname}'

        if dirpath in self.directories:
            operation = {'method': 'remove', 'path': dirpath}
            with open(self.log, 'a') as f:
                f.write(json.dumps(operation) + '\n')

                f.flush()
                os.fsync(f.fileno())
            self.log_entries += 1

            # remove all files in that directory
            for path in self.files.keys():
                if path.startswith(dirpath):
                    self.files.pop(path)
        
            for path in self.directories:
                if path.startswith(dirpath):
                    self.directories.remove(path)
            
            self.directories.remove(dirpath)
        
        reply = {'result': 'Success', 'return': None}
        return json.dumps(reply).encode('utf-8')

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
