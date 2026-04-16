import csv
import os
from Client import Client

def test_path_simple(cwd, input_path, expected_output):
    c = Client('qhynes', 'filesys', False)
    c.handle_command('cwd', [0, cwd])
    _, output = c.handle_command('resolve', [0, input_path])
    return output

def main():
    with open('path_resolution_tests.csv', 'r') as f:
        csv_reader = csv.reader(f)
        fields = next(csv_reader)
        total = 0
        total_correct = 0
        for i, row in enumerate(csv_reader):
            cwd = row[0]
            input_path = row[1]
            expected_output = row[2]
            output = test_path_simple(cwd, input_path, expected_output)
            if output == expected_output:
                # print(f'{cwd:<10} {input_path:<15} -> {expected_output:<15} CORRECT')
                total_correct += 1
            else:
                print(f'{cwd:<10} {input_path:<15} -> {expected_output:<15} INCORRECT ({output})')
            total += 1
        print(f'{total_correct}/{total} passed')

if __name__ == '__main__':
    main()