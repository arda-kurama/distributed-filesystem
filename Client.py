import sys
import socket
import struct
import json
import http.client
import time
import os
import subprocess
import tempfile
import base64
import random
import threading

# magic numbers, should be same on NameServer
HEADER_LEN = 4
LOCK_LEASE = 60
REPLICA_COUNT = 3

class Client:
    def __init__(self, username, project_name, verbose):
        self.username = username
        self.project_name = project_name
        self.path = ''
        self.verbose = verbose
        self.very_verbose = False
    
    # rpc loop methods
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

        if self.very_verbose:
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

    def recv_exact(self, sock, length):
        chunks = []
        bytes_received = 0
        while bytes_received < length:
            chunk = sock.recv(length - bytes_received)
            if chunk == b'':
                raise RuntimeError('Socket connection broken (recv_exact)')
            chunks.append(chunk)
            bytes_received += len(chunk)
        return b''.join(chunks)

    # rpc to communicate directly with storage server
    def rpc_storage_server(self, host, port, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)

        try:
            sock.connect((host, port))

            message_bytes = json.dumps(message).encode('utf-8')
            header = struct.pack('!I', len(message_bytes))
            sock.sendall(header + message_bytes)

            header = self.recv_exact(sock, HEADER_LEN)
            length = struct.unpack('!I', header)[0]
            reply_bytes = self.recv_exact(sock, length)
            return json.loads(reply_bytes.decode('utf-8'))
        finally:
            sock.close()

    # main loop of client    
    def run_shell(self):
        os.system('clear')
        os.system('clear')
        while 1:
            # highlight in different colors
            print(f'\033[1;32m{self.username}@{self.project_name}:\033[1;34m{self.path}\033[0m %', end=' ', flush=True)
            input = sys.stdin.readline().strip()

            args = input.split(' ')
            command = args[0]

            if command == 'exit':
                return
            
            # only print out if reply says so/verbose
            success, message, display = self.handle_command(command, args)
            if display or self.verbose:
                print(message, end='\n' if message != '' else '')
    
    # wrapper that calls individual command methods
    def handle_command(self, command, args):
        match command:
            # list directories/files in current directory
            case 'ls':
                # parse args and validate path
                path = self.parse_args(args, 'usage: ls [path to dir]', self.path)
                if path is None: # parse_args will have already printed error message
                    return False, '', False
                if path is False:
                    return False, 'invalid path', True
                return self.ls(path)

            # change current directory
            case 'cd':
                path = self.parse_args(args, 'usage: cd [path to dir]')
                if path is None:
                    return False, '', False
                if path is False:
                    return False, 'invalid path', True
                return self.cd(path)
            
            # print working directory
            case 'pwd':
                return True, self.path if self.path != '' else '/', True
            
            # create a file
            case 'create':
                path = self.parse_args(args, 'usage: create [path to file]', None)
                if path is None:
                    return False, '', False
                if path is False:
                    return False, 'invalid path', True
                path, _, filename = path.rpartition('/')
                return self.create(path, filename)
            
            # change permissions of file
            case 'chmod':
                if len(args) != 3:
                    return False, 'usage: chmod [path to file] [owner/all]', True
                
                path = self.resolve_path(args[1])
                if path is None:
                    return False, '', False
                if path is False:
                    return False, 'invalid path', True
                
                path, _, filename = path.rpartition('/')
                permissions = args[2]
                return self.chmod(path, filename, permissions)
            
            # remove file
            case 'remove':
                path = self.parse_args(args, 'usage: remove [path to file]', None)
                if path is None:
                    return False, '', False
                if path is False:
                    return False, 'invalid path', True
                path, _, filename = path.rpartition('/')
                return self.remove(path, filename)
            
            # make directory
            case 'mkdir':
                path = self.parse_args(args, 'usage: mkdir [path to dir]', None)
                if path is None:
                    return False, '', False
                if path is False:
                    return False, 'invalid path', True
                path, _, dirname = path.rpartition('/')
                return self.mkdir(path, dirname)
            
            # remove directory
            case 'rmdir':
                path = self.parse_args(args, 'usage: rmdir [path to dir]', None)
                if path is None:
                    return False, '', False
                if path is False:
                    return False, 'invalid path', True
                path, _, dirname = path.rpartition('/')
                return self.rmdir(path, dirname)

            # show what replicas exist of a file
            case 'open':
                path = self.parse_args(args, 'usage: open [path to file]', None)
                if path is None:
                    return False, '', False
                if path is False:
                    return False, 'invalid path', True
                success, message, _ = self.open_for_read(path)
                return success, message, True

            # open file to edit (need permissions/lock)
            case 'vim':
                path = self.parse_args(args, 'usage: vim [path to file]', None)
                if path is None:
                    return False, '', False
                if path is False:
                    return False, 'invalid path', True
                return self.vim(path)
            
            # print out contents of file
            case 'cat':
                path = self.parse_args(args, 'usage: cat [path to file]', None)
                if path is None:
                    return False, '', False
                if path is False:
                    return False, 'invalid path', True
                return self.cat(path)
            
            # clear screen
            case 'clear':
                os.system('clear')
                return True, '', False
            
            # show absolute path of input, for debugging purposes
            case 'resolve':
                path = self.parse_args(args, 'usage: resolve [path]')
                if path is None:
                    return False, '', False
                if path is False:
                    return False, 'invalid path', True
                return True, path, True
            
            # prints directory tree structure on name server
            case 'tree':
                message = {
                    'method': 'tree'
                }

                reply = self.rpc(message)
                return True, '', False
            
            # update's name server checkpoint file
            case 'compact':
                message = {
                    'method': 'compact'
                }

                reply = self.rpc(message)
                return True, '', False

            # prints storage server data on name server
            case 'servers':
                message = {
                    'method': 'servers'
                }

                reply = self.rpc(message)
                return True, '', False
            
            case _:
                return False, 'invalid command', True
    
    # client rpc stubs
    # every command returns success (True/False), along with output, and whether it should be displayed
    def ls(self, path):
        # rpc message with fields name server requires
        message = {
            'method': 'ls',
            'user': self.username,
            'path': path
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result'], True
        else:
            dirs, files = reply['return']

            output = ''
            for dir in dirs:
                output += f'\033[1;32m{dir}\033[0m ' # directories highlighted in green
            for file in files:
                output += file + ' '
        
        return True, output, True

    def cd(self, path):
        message = {
            'method': 'cd',
            'path': path,
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result'], True # always print error messages
        
        self.path = reply['return']
        return True, f'new path: {self.path}', False # only print if verbose

    def create(self, path, filename):
        message = {
            'method': 'create',
            'user': self.username,
            'path': path,
            'filename': filename
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result'], True
        
        return True, f'created file: {filename}', False

    def chmod(self, path, filename, permissions):
        message = {
            'method': 'chmod',
            'user': self.username,
            'path': path,
            'filename': filename,
            'permissions': permissions
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result'], True
        
        return True, f'changed permissions: {permissions}', False

    def remove(self, path, filename):
        message = {
            'method': 'remove',
            'user': self.username,
            'path': path,
            'filename': filename
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result'], True
        
        return True, f'removed file: {filename}', False

    def mkdir(self, path, dirname):
        message = {
            'method': 'mkdir',
            'user': self.username,
            'path': path,
            'dirname': dirname
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result'], True
        
        return True, f'created directory {dirname}', False
    
    def rmdir(self, path, dirname):
        message = {
            'method': 'rmdir',
            'user': self.username,
            'path': path,
            'dirname': dirname
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result'], True
        
        return True, f'removed directory {dirname}', False

    def open_for_read(self, path):
        message = {
            'method': 'open_for_read',
            'path': path,
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result'], True

        return True, reply['return'], False

    def open_for_write(self, path):
        message = {
            'method': 'open_for_write',
            'user': self.username,
            'path': path,
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result'], True

        return True, reply['return'], False
    
    # high level commands, use multiple rpc calls
    def cat(self, path):
        success, replica_data, _ = self.open_for_read(path)
        if not success:
            return False, replica_data, True
        
        file_id = replica_data['file_id']
        replicas = replica_data['replicas']

        success, raw, _ = self.read_from_any_replica(file_id, replicas)
        if not success:
            return False, raw, True
        
        try:
            text = raw.decode('utf-8')
        except UnicodeDecodeError:
            return False, 'file is not valid utf-8 text', True

        return True, text, True

    def vim(self, path):
        success, replica_data, _ = self.create_and_open(path)
        if not success:
            return False, replica_data, True

        file_id = replica_data['file_id']
        replicas = replica_data['replicas']

        # thread for lock renewal
        stop_event = threading.Event()
        renew_thread = threading.Thread(
            target=self.renew_lock,
            args=(path, stop_event),
            daemon=True
        )
        renew_thread.start()

        success, raw, _ = self.read_from_any_replica(file_id, replicas)
        if not success:
            return False, raw, True

        try:
            success, new_raw, _ = self.edit_temp_file(path, raw)
            if not success:
                return False, new_raw, True

            if new_raw == raw:
                return True, f'no changes to file: {path}', False
            
            success, msg, _ = self.write_back(path, new_raw)
            if not success:
                return False, msg, True
            
            return True, f'edited file: {path}', False
        finally:
            stop_event.set()
            reply = self.rpc({'method': 'unlock', 'user': self.username, 'path': path})

            if reply['result'] != 'success':
                return False, f'unlock failed: {reply['result']}', True

    # vim and cat helpers
    def create_and_open(self, path):
        success, open_result, _ = self.open_for_write(path)
        if success:
            return True, open_result, False

        if open_result != 'file not found':
            return False, open_result, True

        # create file if it doesn't exist
        parent, _, filename = path.rpartition('/')
        success, create_result, _ = self.create(parent, filename)
        if not success:
            return False, f'create failed: {create_result}', True

        return self.open_for_write(path)

    def read_from_any_replica(self, file_id, replicas):
        random.shuffle(replicas)
        
        last_error = None
        for replica in replicas:
            try:
                reply = self.rpc_storage_server(replica['host'], replica['port'], {
                    'method': 'read',
                    'id': file_id,
                })
                if reply['result'] == 'success' and reply['return'] is not None:
                    encoded = reply['return']['contents']
                    raw = base64.b64decode(encoded.encode('utf-8'))
                    return True, raw, False
                last_error = reply['result']
            except Exception as e:
                last_error = str(e)
                continue

        return False, f'failed to read from storage server: {last_error}', True
    
    def edit_temp_file(self, path, raw):
        temp_path = None
        try:
            _, _, filename = path.rpartition('/')
            suffix = ''
            if '.' in filename:
                suffix = '.' + filename.split('.')[-1]

            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                temp_path = tmp.name
                tmp.write(raw)
                tmp.flush()
                os.fsync(tmp.fileno())

            subprocess.run(['vim', temp_path], check=False)

            with open(temp_path, 'rb') as f:
                return True, f.read(), False
        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
    
    def write_back(self, path, raw):
        encoded = base64.b64encode(raw).decode('utf-8')

        reply = self.rpc({
            'method': 'write',
            'user': self.username,
            'path': path,
            'contents': encoded,
        })

        if reply['result'] != 'success':
            return False, reply['result'], True

        return True, reply['return'], False
    
    # keeps lock alive while editing in vim
    def renew_lock(self, path, stop_event):
        message = {
            'method': 'lock',
            'user': self.username,
            'path': path
        }
        # periodically renew lock so lease doesn't expire
        while not stop_event.wait(LOCK_LEASE / 2):
            self.rpc(message)

    # miscellaneous helper functions
    def parse_args(self, args, usage_msg, default=''):
        if len(args) > 2:
            print(usage_msg)
            return None
        
        if len(args) == 1:
            if default is None:
                print(usage_msg)
                return None
            else:
                return default
        else:
            return self.resolve_path(args[1])

    def resolve_path(self, input_path):
        input_levels = input_path.rstrip('/').split('/')
        path_levels = self.path.rstrip('/').split('/')
        if input_path.startswith('/'): # absolute path
            resolved_levels = ['']
            for l in input_levels:
                if l == '':
                    continue
                elif l == '.':
                    continue
                elif l == '..':
                    # don't go any higher if at root
                    if resolved_levels[-1] != '':
                        resolved_levels.pop()
                else:
                    if not self.valid_filename(l):
                        return False
                    resolved_levels.append(l)
        else: # relative path
            resolved_levels = path_levels
            for l in input_levels:
                if l == '':
                    continue
                elif l == '.':
                    continue
                elif l == '..':
                    if resolved_levels[-1] != '':
                        resolved_levels.pop()
                else:
                    if not self.valid_filename(l):
                        return False
                    resolved_levels.append(l)
        # print(resolved_levels)
        return '/'.join(resolved_levels)

    def valid_filename(self, filename):
        if filename == '.' or filename == '..':
            return False
        allowed = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ._-0123456789'
        return all(char in allowed for char in filename)

    # def validate_path(self, input_path):
    #     return bool(re.search(r'^(\.|\.\.)?/?(((?!\.{1,2})[\w\.]+|\.\.)/)*(?!\.{1,2})[\w\.]+/?$', input_path))


def main():
    if len(sys.argv) != 3:
        raise RuntimeError('Usage: python Client.py [project_name] [username]')
    
    project_name = sys.argv[1]
    username = sys.argv[2]

    c = Client(username, project_name, False)
    # c.verbose = True
    # c.very_verbose = True
    c.run_shell()

if __name__ == '__main__':
    main()