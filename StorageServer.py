import sys
import socket
import struct
import json
import http.client
import time
import os
import threading
import select
import base64
import hashlib
import uuid

# shared with name server
HEADER_LEN = 4
HEARTBEAT_TIMEOUT = 60
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
        self.prepared = dict()  # txn_id -> prepared write info

        # create socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.hostname = socket.gethostname()
        self.server_socket.bind((self.hostname, port))
        self.server_socket.listen()
        self.server_socket.setblocking(False)
        _, self.port = self.server_socket.getsockname()
        print(f'Listening on {self.hostname}:{self.port}')

        # load identity and register with name server
        self.server_name = f'{self.project_name}-SS_{self.port}'
        self.identity_file = f'{self.server_name}/identity.json'
        identity = self.load_identity()
        if identity:
            self.id = int(identity['id'])
        else:
            # first time starting this storage server
            self.id = None
        self.register() # let name server know we are back/newly joining
        print(f'Registered as storage server {self.id}')

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
    
    # methods to restore name server registry
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
            time.sleep(HEARTBEAT_TIMEOUT / 2)
            if self.verbose:
                print('Sending hearbeat to NameServer...')
            self.rpc(message)

    # checkpoint and log methods
    def playback(self):
        self.data_dir = f'{self.server_name}/data'
        self.tmp_dir = f'{self.server_name}/tmp'
        self.checkpoint = f'{self.server_name}/table.ckpt'
        self.log = f'{self.server_name}/table.txn'
        self.log_entries = 0

        # fresh start
        if not os.path.exists(f'{self.server_name}'):
            os.mkdir(f'{self.server_name}')

            if not os.path.exists(self.log) or \
            not os.path.exists(self.checkpoint) or \
            not os.path.exists(self.data_dir) or \
            not os.path.exists(self.tmp_dir):
                # create relevant files if they dont exist
                if not os.path.exists(self.checkpoint):
                    with open(self.checkpoint, 'a') as _:
                        pass
                if not os.path.exists(self.log):
                    with open(self.log, 'a') as _:
                        pass
                if not os.path.exists(self.data_dir):
                    os.mkdir(self.data_dir)
                if not os.path.exists(self.tmp_dir):
                    os.mkdir(self.tmp_dir)
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
                        if operation['id'] in self.files:
                            meta = self.files[operation['id']]
                            meta.size = operation['size']
                            meta.checksum = operation['checksum']
                            meta.modified = operation['modified']
                    if operation['method'] == 'prepared':
                        self.prepared[operation['txn']] = operation
                    if operation['method'] == 'committed':
                        self.prepared.pop(operation['txn'], None)
                    if operation['method'] == 'aborted':
                        self.prepared.pop(operation['txn'], None)
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
   
    # rpc for communicating with name server
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

    # event driven server methods
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
    
    # cleanup server on close
    def close(self):
        self.epoll.close()
        self.server_socket.close()

    # handles rpc with client
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
        if rpc['method'] == 'read':
            return self.read(rpc.get('id'))
        
        if rpc['method'] == 'prepare_write':
            return self.prepare_write(rpc.get('id'), rpc.get('txn_id'), rpc.get('new_version'), rpc.get('contents'))
        if rpc['method'] == 'commit_write':
            return self.commit_write(rpc.get('txn_id'))
        if rpc['method'] == 'abort_write':
            return self.abort_write(rpc.get('txn_id'))
        
        if rpc['method'] == 'write':
            return self.write(rpc.get('id'), rpc.get('contents'))
        
        # method not found
        return self.make_reply('invalid method')

    # server stubs for rpc with client
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

    def read(self, file_id):
        if file_id is None or file_id not in self.files:
            return self.make_reply('file not found')

        meta = self.files[file_id]
        with open(meta.stored_path, 'rb') as f:
            raw = f.read()

        encoded = base64.b64encode(raw).decode('utf-8')
        return self.make_reply('success', {
            'contents': encoded,
            'size': meta.size,
            'checksum': meta.checksum,
            'modified': meta.modified,
        })

    def prepare_write(self, file_id, txn_id, new_version, contents):
        if file_id is None or file_id not in self.files or txn_id is None or contents is None:
            return self.make_reply('invalid arguments for prepare_write')

        try:
            raw = base64.b64decode(contents.encode('utf-8'))
        except Exception:
            return self.make_reply('invalid contents')

        # write updates to temporary file
        tmp_file = uuid.uuid4().hex
        tmp_path = f'{self.tmp_dir}/{tmp_file}'

        with open(tmp_path, 'wb') as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())

        checksum = hashlib.sha256(raw).hexdigest()

        operation = {
            'method': 'prepared',
            'id': file_id,
            'txn': txn_id,
            'tmp_file': tmp_file,
            'new_version': new_version,
            'size': len(raw),
            'checksum': checksum,
            'modified': time.time(),
        }

        with open(self.log, 'a') as f:
            f.write(json.dumps(operation) + '\n')
            f.flush()
            os.fsync(f.fileno())

        self.log_entries += 1
        self.prepared[txn_id] = operation

        return self.make_reply('success')

    def commit_write(self, txn_id):
        if txn_id is None:
            return self.make_reply('invalid arguments for commit_write')

        if txn_id not in self.prepared:
            return self.make_reply('unknown transaction')

        operation = self.prepared[txn_id]
        file_id = operation['id']

        if file_id not in self.files:
            return self.make_reply('file not found')

        tmp_path = f'{self.tmp_dir}/{operation['tmp_file']}'
        final_path = self.files[file_id].stored_path

        if not os.path.exists(tmp_path):
            return self.make_reply('prepared temp file missing')

        os.replace(tmp_path, final_path)

        log_entry = {
            'method': 'committed',
            'txn': txn_id,
            'id': file_id,
            'size': operation['size'],
            'checksum': operation['checksum'],
            'modified': operation['modified'],
        }

        with open(self.log, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
            f.flush()
            os.fsync(f.fileno())

        self.log_entries += 1

        meta = self.files[file_id]
        meta.size = operation['size']
        meta.checksum = operation['checksum']
        meta.modified = operation['modified']

        self.prepared.pop(txn_id, None)

        return self.make_reply('success')

    def abort_write(self, txn_id):
        if txn_id is None:
            return self.make_reply('invalid arguments for abort_write')

        operation = self.prepared.get(txn_id)

        if operation is not None:
            tmp_path = f'{self.tmp_dir}/{operation["tmp_file"]}'
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        log_entry = {
            'method': 'aborted',
            'txn': txn_id,
        }

        with open(self.log, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
            f.flush()
            os.fsync(f.fileno())

        self.log_entries += 1
        self.prepared.pop(txn_id, None)

        return self.make_reply('success')

    def write(self, file_id, contents):
        if file_id is None or file_id not in self.files or contents is None:
            return self.make_reply('invalid arguments for write')

        try:
            raw = base64.b64decode(contents.encode('utf-8'))
        except Exception:
            return self.make_reply('invalid contents')

        meta = self.files[file_id]
        modified = time.time()
        checksum = hashlib.sha256(raw).hexdigest()
        size = len(raw)

        operation = {
            'method': 'write',
            'id': file_id,
            'size': size,
            'checksum': checksum,
            'modified': modified,
        }

        with open(self.log, 'a') as f:
            f.write(json.dumps(operation) + '\n')
            f.flush()
            os.fsync(f.fileno())
        self.log_entries += 1

        with open(meta.stored_path, 'wb') as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())

        meta.size = size
        meta.checksum = checksum
        meta.modified = modified

        return self.make_reply('success')

    # helper to return json reply
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
    # s.verbose = True
    # s.very_verbose = True
    s.run()

if __name__ == '__main__':
    main()
