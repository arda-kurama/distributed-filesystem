import threading
import time
import random
from Client import Client

PROJECT = 'filesys'
NUM_CLIENTS = 50
REQUESTS_PER_CLIENT = 100

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

def worker():
    global success_count, failure_count

    c = Client('testuser', PROJECT, False)

    for _ in range(REQUESTS_PER_CLIENT):
        start = time.perf_counter()
        try:
            ok, _, _ = c.cat('/test_file.txt')
            elapsed = time.perf_counter() - start

            with lock:
                latencies.append(elapsed)

            if ok:
                with lock:
                    success_count += 1
            else:
                with lock:
                    failure_count += 1
        except Exception:
            elapsed = time.perf_counter() - start
            with lock:
                latencies.append(elapsed)
                failure_count += 1


def main():
    threads = []
    bench_start = time.perf_counter()

    for _ in range(NUM_CLIENTS):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    bench_end = time.perf_counter()
    duration = bench_end - bench_start

    total_requests = NUM_CLIENTS * REQUESTS_PER_CLIENT
    throughput = success_count / duration if duration > 0 else 0

    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)

    print(f'Clients: {NUM_CLIENTS}')
    print(f'Requests/client: {REQUESTS_PER_CLIENT}')
    print(f'Total requests: {total_requests}')
    print(f'Successful: {success_count}')
    print(f'Failed: {failure_count}')
    print(f'Total test time: {duration:.3f} s')
    print(f'Throughput: {throughput:.2f} req/s')
    print(f'Average latency: {avg_latency * 1000:.2f} ms')

    if p50 is not None:
        print(f'P50 latency: {p50 * 1000:.2f} ms')
        print(f'P95 latency: {p95 * 1000:.2f} ms')
        print(f'P99 latency: {p99 * 1000:.2f} ms')

if __name__ == '__main__':
    main()