import matplotlib.pyplot as plt

def convert_results():
    clients = []
    durations = []
    successes = []
    fails = []
    throughputs = []
    latencies = []
    p95s = []
    p99s = []

    with open('name_server_results.txt', 'r') as f:
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

    with open('name_server_results.csv', 'w') as f:
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

    with open('name_server_results.csv', mode='r', newline='') as f:
        fields = f.readline().split(',')
        while (line := f.readline()) != '':
            row = line.strip().split(',')
            clients.append(int(row[0]))
            durations.append(float(row[1]))
            successes.append(int(row[2]))
            fails.append(int(row[3]))
            throughputs.append(float(row[4]))
            latencies.append(float(row[5]))
            p95s.append(float(row[6]))
            p99s.append(float(row[7]))
    
    return clients, durations, successes, fails, throughputs, latencies, p95s, p99s

def graph_results():
    convert_results()
    clients, _, _, _, throughputs, latencies, _, _ = load_results()

    fig, axs = plt.subplots(1, 2, figsize=(10, 4))

    axs[0].plot(clients, latencies, marker='o')
    axs[0].set_title('Latency vs Clients')
    axs[0].set_xlabel('Clients')
    axs[0].set_ylabel('Latency (ms)')
    axs[0].set_xticks(clients)
    axs[0].set_ylim(0, 120)
    axs[0].grid(True)
    axs[0].tick_params(axis='x', rotation=45)

    axs[1].plot(clients, throughputs, marker='o')
    axs[1].set_title('Throughput vs Clients')
    axs[1].set_xlabel('Clients')
    axs[1].set_ylabel('Req/s')
    axs[1].set_xticks(clients)
    axs[1].grid(True)
    axs[1].tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    graph_results()