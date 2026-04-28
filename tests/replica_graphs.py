import matplotlib.pyplot as plt

def load_results(filename):
    clients = []
    latencies = []

    with open(filename, 'r') as f:
        while 1:
            clients.append(int(f.readline().split()[1]))
            f.readline()
            f.readline()
            f.readline()
            f.readline()
            f.readline()
            f.readline()
            latencies.append(float(f.readline().split()[2]))
            f.readline()
            f.readline()
            f.readline()
            
            if f.readline() == '':
                break
        
        return clients, latencies

def graph_results():
    clients1, latencies1 = load_results('1-replica_results.txt')
    clients3, latencies3 = load_results('3-replica_results.txt')

    plt.figure(figsize=(6, 4))

    plt.plot(clients1, latencies1, marker='o', label='1 Replica')
    plt.plot(clients3, latencies3, marker='o', label='3 Replicas')

    plt.title('Latency vs Clients')
    plt.xlabel('Clients')
    plt.ylabel('Latency (ms)')
    plt.xticks(sorted(set(clients1 + clients3)))
    plt.grid(True)
    plt.legend()

    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    graph_results()