import threading
import time
import random
from Client import Client

PROJECT = 'filesys'
NUM_CLIENTS = 25
TEST_DURATION = 120 # seconds

success_count = 0
failure_count = 0
latencies = []
lock = threading.Lock()

def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    idx = min(int((p / 100) * len(values)), len(values) - 1)
    return values[idx]

def worker(stop_time):
    global success_count, failure_count

    c = Client('testuser', PROJECT, False)

    while time.time() < stop_time:
        start = time.perf_counter()
        try:
            ok, _, _ = c.ls('/')
            elapsed = time.perf_counter() - start

            with lock:
                latencies.append(elapsed)
                if ok:
                    success_count += 1
                else:
                    failure_count += 1
        except Exception:
            elapsed = time.perf_counter() - start
            with lock:
                latencies.append(elapsed)
                failure_count += 1

        # simulate user think time between terminal commands
        time.sleep(random.uniform(1.0, 5.0))

def main():
    threads = []
    stop_time = time.time() + TEST_DURATION
    bench_start = time.perf_counter()

    for _ in range(NUM_CLIENTS):
        t = threading.Thread(target=worker, args=(stop_time,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    duration = time.perf_counter() - bench_start
    throughput = success_count / duration if duration > 0 else 0
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)

    print(f'Clients: {NUM_CLIENTS}')
    print(f'Test duration: {duration:.2f}s')
    print(f'Successful: {success_count}')
    print(f'Failed: {failure_count}')
    print(f'Observed throughput: {throughput:.2f} req/s')
    print(f'Average latency: {avg_latency * 1000:.2f} ms')
    if p95 is not None:
        print(f'P95 latency: {p95 * 1000:.2f} ms')
        print(f'P99 latency: {p99 * 1000:.2f} ms')

if __name__ == '__main__':
    main()