import sys
import socket
import struct
import json
import http.client
import time
import hashlib
import os

from NameServer import NameServer

BUFSIZE = 1024
HEADER_LEN = 4

class Client:
    def __init__(self, user_name, project_name, verbose):
        self.user_name = user_name
        self.project_name = project_name
        self.path = ''
        self.verbose = verbose

    def connect(self):
        pass
    
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
            
            self.handle_command(command, args)
    
    def handle_command(self, command, args):
        match command:
            case 'ls':
                self.ls()

            case 'cd':
                if len(args) > 2:
                    print('usage: cd / cd [directory]')
                    return
                
                self.cd(args[1] if len(args) == 2 else None)
                    
            case 'pwd':
                if self.path == '':
                    print('/')
                else:
                    print(self.path)
            
            case 'create':
                if len(args) != 2:
                    print('usage: create [file]')
                    return
                
                self.create(args[1])
            
            case 'remove':
                if len(args) != 2:
                    print('usage: remove [file]')
                    return
                
                self.remove(args[1])
                
            case 'mkdir':
                if len(args) != 2:
                    print('usage: mkdir [directory]')
                    return

                self.mkdir(args[1])
            
            case 'rmdir':
                if len(args) != 2:
                    print('usage: rmdir [directory]')
                    return

                self.rmdir(args[1])

            case 'open':
                if len(args) != 2:
                    print('usage: open [file]')
                    return

                # get storage server to connect to from name server
                pass
            
            case 'clear':
                os.system('clear')
            
            case 'compact':
                message = {
                    'method': 'compact'
                }

                reply = self.rpc(message)
            
            case _:
                print('invalid command')
            
            # vim interface
            # - allows you to read/edit file
            # - can save changes, pass them along to storage servers
    
    def ls(self):
        message = {
            'method': 'ls',
            'path': self.path
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            raise RuntimeError(f'ls error: {reply['result']}')
        else:
            dirs, files = reply['return']

            if len(files) == 0 and len(dirs) == 0:
                return

            for dir in dirs:
                print(f'\033[1;32;40m{dir}\033[0m', end='\t') # for directories
            for file in files:
                print(file, end='\t')
            print()

    def cd(self, dest_dir):
        if dest_dir is None: # go to root directory
            self.path = ''
            if self.verbose:
                print(f'new path: {self.path}')
            return
        
        if dest_dir == '..': # move up directory
            if self.path == '':
                return
            self.path = self.path.rpartition('/')[0]
            if self.verbose:
                print(f'new path: {self.path}')
            return
        
        if not self.valid_filename(dest_dir):
            print(f'directory {dest_dir} does not exist')
            return
        
        message = {
            'method': 'cd',
            'path': self.path,
            'dest_dir': dest_dir
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            print(f'directory {dest_dir} does not exist')
        else:
            self.path = reply['return']
            if self.verbose:
                print(f'new path: {self.path}')

    def create(self, filename):
        message = {
            'method': 'create',
            'path': self.path,
            'filename': filename
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            raise RuntimeError(f'create error: {reply['result']}')
        
        if self.verbose:
            print(f'created file: {filename}')

    def remove(self, filename):
        message = {
            'method': 'remove',
            'path': self.path,
            'filename': filename
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            raise RuntimeError(f'remove error: {reply['result']}')
        
        if self.verbose:
            print(f'removed file: {filename}')

    def mkdir(self, dirname):
        if not self.valid_filename(dirname):
            print(f'{dirname} not a valid directory name')
            return
        
        message = {
            'method': 'mkdir',
            'path': self.path,
            'dirname': dirname
        }

        reply = self.rpc(message)

        if reply['result'] != 'success':
            raise RuntimeError(f'mkdir error: {reply['result']}')
        
        if self.verbose:
            print(f'created directory {dirname}')
    
    def rmdir(self, dirname):
        if not self.valid_filename(dirname):
            print(f'{dirname} not a valid directory name')
            return
        
        message = {
            'method': 'rmdir',
            'path': self.path,
            'dirname': dirname
        }

        reply = self.rpc(message)
        
        if reply['result'] != 'success':
            raise RuntimeError(f'rmdir error: {reply['result']}')
        
        if self.verbose:
            print(f'removed directory {dirname}')

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

    def valid_filename(self, filename):
        allowed = 'abcdefhijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ._-0123456789'
        return all(char in allowed for char in filename)


def main():
    if len(sys.argv) != 2:
        raise RuntimeError('Usage: python Client.py [project_name]')

    # if len(sys.argv) == 4 and sys.argv[2] == '-test':
    #     testfile = sys.argv[3]
    

    
    c = Client('qhynes', sys.argv[1], False)
    c.very_verbose = False
    c.run_shell()

if __name__ == '__main__':
    main()