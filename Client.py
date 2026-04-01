import sys
import socket
import struct
import json
import http.client
import time
import hashlib

from NameServer import NameServer

BUFSIZE = 1024

class Client:
    def __init__(self, project_name, name_server):
        self.project_name = project_name
        self.name_server = name_server
        self.path = ''

        # keep a local list of files
        # what happens if another user adds file to directory?
        self.current_directory_files = None

    def connect(self):
        pass
    
    def run_shell(self):
        while 1:
            input = sys.stdin.readline().strip()

            args = input.split(' ')
            command = args[0]

            # print(f'command: {command}')
            # print(f'args: {args}')
            self.handle_command(command, args)
    
    def handle_command(self, command, args):
        # ls, cd, pwd, create, mkdir, open, remove
        match command:
            case 'ls':
                if not self.current_directory_files:
                    # get list of files in current directory from name server
                    self.current_directory_files = self.name_server.ls(self.path)
                print('Files: ', end='')
                print(*self.current_directory_files)
            case 'cd':
                if len(args) == 1 and self.path != '': # go to root directory
                    self.path = ''
                    self.current_directory_files = None
                    print(f'New path: {self.path}')
                elif len(args) == 2:
                    if args[1] == '..' and self.path != '': # move up directory
                        self.path = self.path.rpartition('/')[0]
                        self.current_directory_files = None
                        print(f'New path: {self.path}')
                    else:
                        # make sure argument is valid name (no spaces, other weird characters)
                        # to-do

                        # check with name server if argument is a directory
                        # to-do

                        # return status, also list of files/directory in new directory?
                        
                        self.path = f'{self.path}/{args[1]}'
                        self.current_directory_files = None
                        print(f'New path: {self.path}')
                else:
                    print('Usage: cd [directory]')
            case 'pwd':
                    print(f'Path: {self.path}')
            case 'create':
                if len(args) == 2:
                    # create file on name server
                    if self.name_server.create(self.path, args[1]):
                        print(f'Created file {args[1]}')
                        if self.current_directory_files:
                            self.current_directory_files.append(args[1])
                    else:
                        print('File already exists/failed to create')
                else:
                    print('Usage: create [file]')
            case 'mkdir':
                if len(args) == 2:
                    # make sure argument is valid name (no spaces, other weird characters)
                    
                    # create directory on name server
                    if self.name_server.mkdir(self.path, args[1]):
                        print(f'Created file {args[1]}')
                        if self.current_directory_files:
                            self.current_directory_files.append(args[1])
                    else:
                        print('Directory already exists/failed to create')
                    pass
                else:
                    print('Usage: mkdir [directory]')
            case 'open':
                if len(args) == 2:
                    # get storage server to connect to from name server
                    pass
                else:
                    print('Usage: open [file]')
            case 'remove':
                if len(args) == 2:
                    # remove entry from name server
                    if self.name_server.remove(args[1]):
                        if self.current_directory_files:
                            self.current_directory_files.remove(args[1])
                    else:
                        print('File did not exist/failed to remove')
                    pass
                else:
                    print('Usage: remove [file]')
            
            # vim interface
            # - allows you to read/edit file
            # - can save changes, pass them along to storage servers

def main():
    project = 'filesys'
    n = NameServer(project)
    c = Client(project, n)
    c.run_shell()

if __name__ == '__main__':
    main()