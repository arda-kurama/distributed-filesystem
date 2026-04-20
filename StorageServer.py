import sys
import socket
import struct
import json
import http.client
import time
import os
import re
import threading
import select

from NameServer import NameServer

HEADER_LEN = 4
HEARTRATE = 30
BUFSIZE = 4096

class StoredFile:
    def __init__(self, id, stored_path, size, checksum, modified):
        self.id = id
        self.stored_path = stored_path
        self.size = size
        self.checksum = checksum
        self.modified = modified # time last modified

class StorageServer:
    def __init__(self, project_name, port, verbose):
        self.project_name = project_name
        self.verbose = verbose
        self.very_verbose = False
        self.files = dict() # id -> StoredFile()

        # create socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.hostname = socket.gethostname()
        self.port = port
        self.server_socket.bind((self.hostname, self.port))
        self.server_socket.listen()
        self.server_socket.setblocking(False)
        print(f'Listening on {self.hostname}:{self.port}')

        # load identity and register with name server
        self.server_name = f'{self.project_name}-SS_{self.port}'
        self.identity_file = f'{self.server_name}/identity.json'
        identity = self.load_identity()
        if identity:
            self.id = identity['id']
        else:
            # first time starting this storage server
            self.id = None
        self.register() # let name server know we are back/newly joining
        print(f'Registered as: {self.server_name}')

        self.playback()
        self.save_identity({'id': self.id, 'host': self.hostname, 'port': self.port})        

        # set up polling for event driven
        self.epoll = select.epoll()
        self.epoll.register(self.server_socket.fileno(), select.EPOLLIN)
        self.client_sockets = dict()
        self.client_read_buffers = dict()
        self.client_write_buffers = dict()
        self.client_msglens = dict()
        self.client_bytes = dict()
    
    def load_identity(self):
        if os.path.exists(self.identity_file):
            with open(self.identity_file, 'r') as f:
                return json.load(f)
        return None

    def save_identity(self, identity):
        with open(self.identity_file, 'w') as f:
            json.dump(identity, f)
            f.flush()
            os.fsync(f.fileno())
    
    def connect(self, hostname, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((hostname, port))

        if self.verbose:
            print(f'Connected to {hostname} on port {port}')
    
    def lookup_server_name(self):
        conn = http.client.HTTPConnection('catalog.cse.nd.edu', 9097)
        conn.request('GET', '/query.json')
        response = conn.getresponse()
        data = response.read().decode('utf-8')

        entries = json.loads(data)
        server = None
        time = 0
        for entry in entries:
            if entry.get('type') == 'name_server' and entry.get('project') == self.project_name:
                if entry['lastheardfrom'] > time:
                    server = entry
                    time = entry['lastheardfrom']

        if not server:
            raise RuntimeError(f'Error: no name server found with name \'{self.project_name}\'')
        
        return (server['name'], server['port'])
    
    def send_message(self, message):
        header = struct.pack('!I', len(message))
        message = header + message
        message_length = len(message)
        
        bytes_sent = 0
        while bytes_sent < message_length:
            sent = self.sock.send(message[bytes_sent:])

            if sent == 0:
                raise RuntimeError('Socket connection broken (send)')
            bytes_sent += sent
    
    def recv(self, length):
        chunks = []
        bytes_recieved = 0
        while bytes_recieved < length:
            chunk = self.sock.recv(length - bytes_recieved)
            
            if chunk == b'':
                raise RuntimeError('Socket connection broken (recv)')
            chunks.append(chunk)
            bytes_recieved += len(chunk)
        return b''.join(chunks)
    
    def recv_message(self):
        header = self.recv(HEADER_LEN)
        message_length = struct.unpack('!I', header)[0]
        return self.recv(message_length)

    def rpc(self, message):
        self.sock = None
        reply = None
        attempts = 0

        while not reply:
            try:
                # lookup
                if self.very_verbose:
                    print('Looking up name...')
                hostname, port = self.lookup_server_name()
                
                # connect
                if self.very_verbose:
                    print('Connecting to server...')
                self.connect(hostname, port)
                self.sock.settimeout(5.0)
        
                # send
                if self.very_verbose:
                    print('Sending message...')
                message_bytes = json.dumps(message).encode('utf-8')
                self.send_message(message_bytes)
                # if self.verbose:
                #     print(f'Sent: {message}')

                # recv
                if self.very_verbose:
                    print('Receiving response...')
                reply_bytes = self.recv_message()
                reply = json.loads(reply_bytes.decode('utf-8'))
                # if self.verbose:
                #     print(f'Recv: {reply}')
                
                # close
                self.sock.close()
            
            except Exception as e:
                if self.sock:
                    self.sock.close()

                if self.verbose:
                    print(f'Error: {e}')
                    print(f'{2 ** attempts} second timeout...')
                
                time.sleep(2 ** attempts)
                attempts += 1

        return reply
    
    def register(self):
        message = {
            'method': 'register',
            'id': self.id, # None on initial startup
            'host': self.hostname,
            'port': self.port
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            raise ConnectionRefusedError(reply['result'])
        
        self.id = reply['return']
        return self.id
    
    def heartbeat(self):
        message = {
            'method': 'heartbeat',
            'id': self.id
        }
        # periodically let name server know we're still here
        while True:
            time.sleep(HEARTRATE)
            self.rpc(message)
    
    def playback(self):
        self.data_dir = f'{self.server_name}/data'
        self.checkpoint = f'{self.server_name}/table.ckpt'
        self.log = f'{self.server_name}/table.txn'
        self.log_entries = 0

        # fresh start
        if not os.path.exists(f'{self.server_name}'):
            os.mkdir(f'{self.server_name}')

            if not os.path.exists(self.log) or \
            not os.path.exists(self.checkpoint) or \
            not os.path.exists(self.data_dir):
                # create relevant files if they dont exist
                if not os.path.exists(self.checkpoint):
                    with open(self.checkpoint, 'a') as _:
                        pass
                if not os.path.exists(self.log):
                    with open(self.log, 'a') as _:
                        pass
                if not os.path.exists(self.data_dir):
                    os.mkdir(self.data_dir)
            return
        
        # load checkpoint into memory
        try:
            with open(self.checkpoint, 'r') as f:
                contents = json.load(f)
                for id, meta in contents.items():
                    self.files[id] = StoredFile(
                        id=meta['id'],
                        stored_path=meta['stored_path'],
                        size=meta['size'],
                        checksum=meta['checksum'],
                        modified=meta['modified']
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

                    if operation['method'] == 'create':
                        self.files[operation['id']] = StoredFile(
                            id=operation['id'],
                            stored_path=operation['stored_path'],
                            size=operation['size'],
                            checksum=operation['checksum'],
                            modified=operation['modified']
                        )
                    if operation['method'] == 'remove':
                        self.files.pop(operation['id'])
                    if operation['method'] == 'write':
                        # write, file, position, data
                        pass
                    if operation['method'] == 'delete':
                        # delete, file, position, length
                        pass
        except json.JSONDecodeError:
            # empty log file
            pass

        # remove any files on disk that are no longer in memory
        self.clean_orphans()
    
    def compact(self):
        json_files = dict()
        for id, meta in self.files.items():
            json_files[id] = {
                'id': meta.id,
                'stored_path': meta.stored_path,
                'size': meta.size,
                'checksum': meta.checksum,
                'modified': meta.modified
            }

        # write current data to new checkpoint
        tmp = f'{self.checkpoint}.tmp'
        with open(tmp, 'w') as f:
            json.dump(json_files, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        
        # atomically update checkpoint file
        self.log_entries = 0
        os.rename(tmp, self.checkpoint)

        # clear log file
        os.remove(self.log)
        with open(self.log, 'a') as _:
            pass
    
    def clean_orphans(self):
        # get dict of stored paths for files
        active_files = {meta.stored_path for meta in self.files.values()}

        # remove any files on disk that aren't in ht
        for name in os.listdir(self.data_dir):
            path = os.path.join(self.data_dir, name)
            if path not in active_files:
                os.remove(path)
    
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

    def response(self, message):
        rpc = json.loads(message.decode('utf-8'))

        # check if message is correctly formatted
        if 'method' not in rpc:
            return self.make_reply('invalid message, no method provided')
        
        # methods
        if rpc['method'] == 'create':
            return self.create(rpc.get('id'), rpc.get('path'))
        if rpc['method'] == 'remove':
            return self.remove(rpc.get('id'))
        if rpc['method'] == 'stat':
            return self.stat(rpc.get('id'))
        
        # method not found
        return self.make_reply('invalid method')

    def create(self, file_id, path):
        if file_id is None or path is None:
            return self.make_reply('invalid arguments for create')

        if file_id in self.files:
            return self.make_reply('success')  # idempotent

        stored_path = f'{self.data_dir}/{file_id}'

        operation = {
            'method': 'create',
            'id': file_id,
            'stored_path': stored_path,
            'size': 0,
            'checksum': None,
            'modified': time.time()
        }

        with open(self.log, 'a') as f:
            f.write(json.dumps(operation) + '\n')
            f.flush()
            os.fsync(f.fileno())
        self.log_entries += 1

        with open(stored_path, 'wb') as f:
            f.flush()
            os.fsync(f.fileno())

        self.files[file_id] = StoredFile(
            id=file_id,
            stored_path=stored_path,
            size=0,
            checksum=None,
            modified=operation['modified']
        )

        return self.make_reply('success')

    def remove(self, file_id):
        if file_id is None:
            return self.make_reply('invalid arguments for remove')

        if file_id not in self.files:
            return self.make_reply('success')  # idempotent

        operation = {
            'method': 'remove',
            'id': file_id
        }

        with open(self.log, 'a') as f:
            f.write(json.dumps(operation) + '\n')
            f.flush()
            os.fsync(f.fileno())
        self.log_entries += 1

        stored_path = self.files[file_id].stored_path
        if os.path.exists(stored_path):
            os.remove(stored_path)

        self.files.pop(file_id)
        return self.make_reply('success')

    def stat(self, file_id):
        if file_id is None or file_id not in self.files:
            return self.make_reply(f'file id {file_id} found')

        meta = self.files[file_id]
        return self.make_reply('success', {
            'id': meta.id,
            'stored_path': meta.stored_path,
            'size': meta.size,
            'checksum': meta.checksum,
            'modified': meta.modified
        })

    def make_reply(self, result, value=None):
        return json.dumps({'result': result, 'return': value}).encode('utf-8')

    def run(self):
        threading.Thread(target=self.heartbeat, daemon=True).start()

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
    if len(sys.argv) != 3:
        raise RuntimeError('Usage: python StorageServer.py [project_name] [port]')
    
    project_name = sys.argv[1]
    port = int(sys.argv[2])

    s = StorageServer(project_name, port, False)
    s.run()

if __name__ == '__main__':
    main()
