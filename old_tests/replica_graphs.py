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

    fig, axs = plt.subplots(1, 2, figsize=(10, 4))

    axs[0].plot(clients1, latencies1, marker='o')
    axs[0].set_title("Latency vs Clients")
    axs[0].set_xlabel("Clients")
    axs[0].set_ylabel("Latency (ms)")
    axs[0].set_xticks(clients1)
    axs[0].grid(True)

    axs[0].plot(clients3, latencies3, marker='o')

    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    graph_results()