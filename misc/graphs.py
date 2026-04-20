def convert_results():
    clients = []
    durations = []
    successes = []
    fails = []
    throughputs = []
    latencies = []
    p95s = []
    p99s = []

    with open('results.txt', 'r') as f:
        while 1:
            clients.append(int(f.readline().split()[1]))
            durations.append(float(f.readline().split()[2][:-1]))
            successes.append(int(f.readline().split()[1]))
            fails.append(int(f.readline().split()[1]))
            throughputs.append(float(f.readline().split()[2]))
            latencies.append(float(f.readline().split()[2]))
            p95s.append(float(f.readline().split()[2]))
            p99s.append(float(f.readline().split()[2]))
            if f.readline() == '':
                break

    with open('results.csv', 'w') as f:
        f.write('Clients,Duration,Success,Fail,Throughput,Latency,P95,P99\n')
        for i in range(len(clients)):
            f.write(f'{clients[i]},{durations[i]},{successes[i]},{fails[i]},{throughputs[i]},{latencies[i]},{p95s[i]},{p95s[i]}\n')

def load_results():
    clients = []
    durations = []
    successes = []
    fails = []
    throughputs = []
    latencies = []
    p95s = []
    p99s = []

    with open('results.csv', mode='r', newline='') as f:
        fields = f.readline().split(',')
        while (line := f.readline()) != '':
            row = line.strip().split(',')
            clients.append(row[0])
            durations.append(row[1])
            successes.append(row[2])
            fails.append(row[3])
            throughputs.append(row[4])
            latencies.append(row[5])
            p95s.append(row[6])
            p99s.append(row[7])
    
    return clients, durations, successes, fails, throughputs, latencies, p95s, p99s

def graph_results():
    clients, _, _, _, throughputs, latencies, _, _ = load_results()
    print(clients)

if __name__ == '__main__':
    graph_results()