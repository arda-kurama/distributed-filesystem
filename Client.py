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

class Client:
    def __init__(self, project_name, name_server, verbose):
        self.project_name = project_name
        self.name_server = name_server
        self.path = ''
        self.verbose = verbose

        # keep a local list of files
        # what happens if another user adds file to directory?
        self.current_directory_files = None

    def connect(self):
        pass
    
    def run_shell(self):
        os.system('clear')
        os.system('clear')
        while 1:
            print('user@dist_filesys %', end=' ', flush=True)
            input = sys.stdin.readline().strip()

            args = input.split(' ')
            command = args[0]
            
            self.handle_command(command, args)
    
    def handle_command(self, command, args):
        match command:
            case 'ls':
                if not self.current_directory_files:
                    # get list of files in current directory from name server
                    self.current_directory_files = self.name_server.ls(self.path)

                if len(self.current_directory_files) != 0:
                    print(*self.current_directory_files)

            case 'cd':
                if len(args) == 1 and self.path != '': # go to root directory
                    if self.path != '':
                        self.current_directory_files = None
                    
                    self.path = ''
                    if self.verbose:
                        print(f'new path: {self.path}')
                    return
                
                if len(args) != 2:
                    print('usage: cd [directory]')
                    return
                
                dest_dir = args[1]
                if dest_dir == '..': # move up directory
                    if self.path == '':
                        return
                    self.path = self.path.rpartition('/')[0]
                    self.current_directory_files = None
                    if self.verbose:
                        print(f'new path: {self.path}')
                else:
                    if not self.valid_filename(dest_dir):
                        print(f'directory {dest_dir} does not exist')
                        return
                    
                    if self.name_server.cd(self.path, dest_dir):
                        self.path = f'{self.path}/{dest_dir}'
                        self.current_directory_files = None
                        if self.verbose:
                            print(f'new path: {self.path}')
                    else:
                        print(f'directory {dest_dir} does not exist')
                    
            case 'pwd':
                if self.path == '':
                    print('/')
                else:
                    print(self.path)
            
            case 'create':
                if len(args) != 2:
                    print('usage: create [file]')
                    return
                
                # is this idempotent?
                file = args[1]
                self.name_server.create(self.path, file)
                if self.verbose:
                    print(f'created file {file}')
                if self.current_directory_files:
                    self.current_directory_files.append(file)
            
            case 'remove':
                if len(args) != 2:
                    print('usage: remove [file]')
                    return

                filename = args[1]

                self.name_server.remove(filename)
                if self.verbose:
                    print(f'removed file {filename}')
                if self.current_directory_files:
                    self.current_directory_files.remove(filename)
                
            case 'mkdir':
                if len(args) != 2:
                    print('usage: mkdir [directory]')
                    return

                dirname = args[1]

                if not self.valid_filename(dirname):
                    print(f'{dirname} not a valid directory name')
                    return
                
                self.name_server.mkdir(self.path, dirname)
                if self.verbose:
                    print(f'created directory {dirname}')
                if self.current_directory_files:
                    self.current_directory_files.append(dirname)
            
            case 'mkdir':
                if len(args) != 2:
                    print('usage: rmdir [directory]')
                    return

                dirname = args[1]

                if not self.valid_filename(dirname):
                    print(f'{dirname} not a valid directory name')
                    return
                
                self.name_server.rmdir(self.path, dirname)
                if self.verbose:
                    print(f'removed directory {dirname}')
                if self.current_directory_files:
                    self.current_directory_files.remove(dirname)

            case 'open':
                if len(args) != 2:
                    print('usage: open [file]')
                    return

                # get storage server to connect to from name server
                pass
            
            case 'clear':
                os.system('clear')
            
            case _:
                print('invalid command')
            
            # vim interface
            # - allows you to read/edit file
            # - can save changes, pass them along to storage servers
    
    def valid_filename(self, filename):
        allowed = 'abcdefhijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ._-'
        return all(char in allowed for char in filename)


def main():
    project = 'filesys'
    n = NameServer(project)
    c = Client(project, n, True)
    c.run_shell()

if __name__ == '__main__':
    main()