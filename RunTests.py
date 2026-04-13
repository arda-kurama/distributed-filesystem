# These test cases are shell-command transcripts for your DFS client.

# Assumptions
# - Each inputN.txt is fed to the client as stdin.
# - Each outputN.txt contains only the meaningful expected stdout lines.
# - Ignore prompts, ANSI colors, and screen-clearing output from `clear`.
# - For `ls`, expected names are space-separated on one line in sorted order.
# - Empty output files mean the commands should succeed without printing anything meaningful.

# Coverage
# - 0: initial root state
# - 1: mkdir / cd / pwd / ls
# - 2: create / remove file
# - 3: multiple files in one directory
# - 4: file-vs-directory name collision
# - 5: error paths for mkdir/remove/cd
# - 6: nested directories and ls at different levels
# - 7: recursive rmdir
# - 8: cd .. and pwd behavior
# - 9: wrong-command-type errors for remove/rmdir

# You will probably want a small harness that:
# 1. strips prompts,
# 2. strips ANSI escape sequences,
# 3. drops blank lines,
# 4. compares normalized stdout to outputN.txt.

import subprocess
import pathlib
import re
import time

TEST_DIR = pathlib.Path('tests')
SERVER_CMD = ['python', 'NameServer.py', 'filesys']
CLIENT_CMD = ['python', 'Client.py', 'filesys']

def normalize_output(text):
    # remove ANSI escape codes
    text = re.sub(r'\x1b\[[0-9;]*m', '', text)

    lines = []
    for line in text.splitlines():
        line = line.strip()

        # skip empty lines
        if not line:
            continue

        # skip prompts like: qhynes:/docs %
        if re.match(r'^[^:]+:.*%$', line):
            continue

        lines.append(line)

    return '\n'.join(lines).strip()

def run_test(input_file, output_file):
    expected = output_file.read_text().strip()

    proc = subprocess.run(
        CLIENT_CMD,
        stdin=input_file.open('r'),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    actual = normalize_output(proc.stdout)
    passed = (actual == expected)

    if not passed:
        print(f'FAIL: {input_file.name}')
        print('Expected:')
        print(repr(expected))
        print('Actual:')
        print(repr(actual))
        if proc.stderr:
            print('stderr:')
            print(proc.stderr)
    else:
        print(f'PASS: {input_file.name}')

    return passed

def start_server():
    proc = subprocess.Popen(
        SERVER_CMD,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # crude readiness wait
    time.sleep(1)

    return proc

def stop_server(proc):
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()

def main():
    all_passed = True

    server = start_server()

    try:
        for i in range(10):
            input_file = TEST_DIR / f'input{i}.txt'
            output_file = TEST_DIR / f'output{i}.txt'

            if not input_file.exists() or not output_file.exists():
                print(f'Missing test files for case {i}')
                all_passed = False
                continue

            passed = run_test(input_file, output_file)
            all_passed = all_passed and passed

        if all_passed:
            print('\nAll tests passed.')
        else:
            print('\nSome tests failed.')
    finally:
        stop_server(server)

if __name__ == '__main__':
    main()
