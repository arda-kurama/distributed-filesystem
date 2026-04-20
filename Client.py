import sys
import socket
import struct
import json
import http.client
import time
import os
import re

from NameServer import NameServer

HEADER_LEN = 4

class Client:
    def __init__(self, user_name, project_name, verbose):
        self.user_name = user_name
        self.project_name = project_name
        self.path = ''
        self.verbose = verbose
        self.very_verbose = False
    
    def run_shell(self):
        os.system('clear')
        os.system('clear')
        while 1:
            print(f'{self.user_name}:{self.path} %', end=' ', flush=True)
            input = sys.stdin.readline().strip()

            args = input.split(' ')
            command = args[0]

            if command == 'exit':
                return
            
            successful, output = self.handle_command(command, args)
            # print(successful, output)
            print(output, end='\n' if output != '' else '')
            # if self.verbose or not successful:
            #     print(output, end='\n' if output != '' else '')
    
    def handle_command(self, command, args):
        match command:
            case 'ls':
                path = self.parse_args(args, 'usage: ls [path to dir]', self.path)
                if path is False:
                    return False, 'invalid path'
                return self.ls(path)

            case 'cd':
                path = self.parse_args(args, 'usage: cd [path to dir]')
                if path is False:
                    return False, 'invalid path'
                return self.cd(path)
                    
            case 'pwd':
                return True, self.path if self.path != '' else '/'
            
            case 'create':
                path = self.parse_args(args, 'usage: create [path to file]', None)
                if path is False:
                    return False, 'invalid path'
                path, _, filename = path.rpartition('/')
                return self.create(path, filename)
            
            case 'remove':
                path = self.parse_args(args, 'usage: remove [path to file]', None)
                if path is False:
                    return False, 'invalid path'
                path, _, filename = path.rpartition('/')
                return self.remove(path, filename)
                
            case 'mkdir':
                path = self.parse_args(args, 'usage: mkdir [path to dir]', None)
                if path is False:
                    return False, 'invalid path'
                path, _, dirname = path.rpartition('/')
                return self.mkdir(path, dirname)
            
            case 'rmdir':
                path = self.parse_args(args, 'usage: rmdir [path to dir]', None)
                if path is False:
                    return False, 'invalid path'
                path, _, dirname = path.rpartition('/')
                return self.rmdir(path, dirname)

            case 'open':
                path = self.parse_args(args, 'usage: open [path to file]', None)
                if path is False:
                    return False, 'invalid path'

                # get storage server to connect to from name server
                # vim interface
                # - allows you to read/edit file
                # - can save changes, pass them along to storage servers

                return self.open(path)
            
            case 'clear':
                os.system('clear')
                return True, ''
            
            case 'cwd':
                self.path = self.parse_args(args, 'usage: cwd [absolute path]')
                return True, f'new path: {self.path}'
            
            case 'resolve':
                path = self.parse_args(args, 'usage: resolve [path]')
                if path is False:
                    return False, 'invalid path'
                return True, path
            
            case 'tree':
                message = {
                    'method': 'tree'
                }

                reply = self.rpc(message)
                return True, ''
            
            case 'compact':
                message = {
                    'method': 'compact'
                }

                reply = self.rpc(message)
                return True, ''

            case 'servers':
                message = {
                    'method': 'servers'
                }

                reply = self.rpc(message)
                return True, ''
            
            case _:
                return False, 'invalid command'
    
    # every command returns status (True/False), along with output
    def ls(self, path):
        message = {
            'method': 'ls',
            'path': path
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, f'ls error: {reply['result']}'
        else:
            dirs, files = reply['return']

            output = ''
            for dir in dirs:
                output += f'\033[1;32;40m{dir}\033[0m ' # for directories
            for file in files:
                output += file + ' '
        
        return True, output

    def cd(self, path):
        message = {
            'method': 'cd',
            'path': path,
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result']
        
        self.path = reply['return']
        return True, f'new path: {self.path}'

    def create(self, path, filename):
        message = {
            'method': 'create',
            'path': path,
            'filename': filename
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result']
        
        return True, f'created file: {filename}'

    def remove(self, path, filename):
        message = {
            'method': 'remove',
            'path': path,
            'filename': filename
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result']
        
        return True, f'removed file: {filename}'

    def mkdir(self, path, dirname):
        message = {
            'method': 'mkdir',
            'path': path,
            'dirname': dirname
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result']
        
        return True, f'created directory {dirname}'
    
    def rmdir(self, path, dirname):
        message = {
            'method': 'rmdir',
            'path': path,
            'dirname': dirname
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result']
        
        return True, f'removed directory {dirname}'

    def open(self, path):
        message = {
            'method': 'open',
            'path': path,
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            return False, reply['result']

        print(reply['return'])
        
        return True, f'opened file {path}'

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
    
    def parse_args(self, args, usage_msg, default=''):
        if len(args) > 2:
            print(usage_msg)
            return False
        
        if len(args) == 1:
            if default is None:
                print(usage_msg)
                return False
            else:
                return default
        else:
            return self.resolve_path(args[1])

    # def validate_path(self, input_path):
    #     return bool(re.search(r'^(\.|\.\.)?/?(((?!\.{1,2})[\w\.]+|\.\.)/)*(?!\.{1,2})[\w\.]+/?$', input_path))

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


def main():
    if len(sys.argv) != 2:
        raise RuntimeError('Usage: python Client.py [project_name]')

    c = Client('qhynes', sys.argv[1], False)
    # c.very_verbose = False
    c.run_shell()

if __name__ == '__main__':
    main()