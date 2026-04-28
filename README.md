# distributed-filesystem

By Quinn Hynes and Arda Kurama

### Description of System

Final project for CSE 40771 - Scalable Distributed Filesystem with Replicated Storage

File | Job
--- | ---
NameServer.py | coordinates the entire system, handling Client commands and connecting them with StorageServers
StorageServer.py | nodes where file replicas are actually stored
Client.py | interacts with system, providing CLI and file editing
tests | directory full of old tests and code to generate performance graphs. some may no longer work because they ran on an older version of the system

### Running the System
This system was created and tested on the ND CSE student machines, so we recommend running and testing it there. First you must start the NameServer:

```python
python NameServer.py [project_name]
```

Then, start at least 3 StorageServers on different ports:

```python
python StorageServer.py [project_name] [port_1]
python StorageServer.py [project_name] [port_2]
...
python StorageServer.py [project_name] [port_N]
```

Finally, start the Client:

```python
python Client.py [project_name] [username]
```

Once you start the client, you will automatically be placed in the CLI where you can begin running commands. The home directory will be empty, but you can create files, directories, and navigate it like any other terminal. Below is a list of the commands you can run and what they accomplish:

Command | Functionality
--- | ---
ls [path] | lists files and directories in given directory
cd [path] | changes current directory
pwd | print working directory
create [path] | creates a file at given path
chmod [path] [owner/all] | changes permissions of file
remove [path] | removes file at given path
mkdir [path] | creates a directory at given path
rmdir [path] | removes a directory and its contents recursively
open [path] | shows where replicas for a file are located
vim [path] | opens a file to edit
cat [path] | prints out contents of file
clear | clears screen
exit | exits CLI and Client.py program

Commands used for debugging purposes

Command | Functionality
--- | ---
resolve [path] | shows resolved absolute path of input
tree | prints directory tree structure on name server
compact | compacts the NameServer's checkpoint file
servers | prints StorageSerer data on NameServer

The Client can exit, NameServer and StorageServers can crash and restart, but if the Client rejoins the same project it will see the updated directory and files. If you wish to see more debug and status messages, you can uncomment the lines in NameServer.py, StorageServer.py, and Client.py that say things like:

```python
c.verbose = True
c.very_verbose = True
```

## Cleaning up the Sytem

If you want to clean up the entire system after running, simply:

- Kill the StorageServers and NameServer
- Remove directories that have been created

```bash
rm -rf [project_name]-*
```

And the project will be removed. If you restart the system with the same project name, the home directory will be clean.