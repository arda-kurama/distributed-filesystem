import csv
import os
import subprocess
import time
from Client import Client

def start_server(project):
    proc = subprocess.Popen(
        ["python3", "NameServer.py", project],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2) # wait for it to start up/register
    return proc

def stop_server(proc):
    if proc:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except:
            proc.kill()


def test_command(client, command):
    args = command.split(' ')
    command = args[0]
    return client.handle_command(command, args)


def main():
    with open('client_command_tests.csv', 'r') as f:
        csv_reader = csv.reader(f)
        fields = next(csv_reader)

        current_test_id = None
        server = None

        total = 0
        total_correct = 0

        for i, row in enumerate(csv_reader):
            test_id = row[0]
            command = row[2]
            expected_success = row[3] == 'True'
            expected_output = row[4]

            # restart server when test_id changes
            if test_id != current_test_id:
                stop_server(server)

                project = f'filesys_{test_id}'

                # wipe old state
                if os.path.exists(project):
                    os.system(f'rm -rf {project}')

                server = start_server(project)
                c = Client('qhynes', project, False)

                current_test_id = test_id

            success, output = test_command(c, command)

            # print results
            if success == expected_success and output == expected_output:
                print(f'{i}: {command:<25} -> ({success}, {output}) CORRECT')
                total_correct += 1
            else:
                print(f'{i}: {command:<25} -> ({success}, {output}) INCORRECT, expected ({expected_success}, {expected_output})')

            total += 1

        stop_server(server)

        print(f'{total_correct}/{total} passed')


if __name__ == '__main__':
    main()