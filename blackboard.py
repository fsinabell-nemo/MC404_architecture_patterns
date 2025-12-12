from __future__ import annotations
import math
import time
from multiprocessing import Process, Manager, Lock
from typing import List, Tuple


def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n in (2, 3):
        return True
    if n % 2 == 0:
        return False
    r = int(math.isqrt(n))
    for f in range(3, r + 1, 2):
        if n % f == 0:
            return False
    return True


def worker_blackboard(bb, bb_lock: Lock, worker_id: int) -> None:
    while True:
        with bb_lock:
            if bb["done"]:
                return
            tasks = bb["tasks"]
            if len(tasks) == 0:
                bb["done"] = True
                return
            lo, hi = tasks.pop()

        local_primes: List[int] = []
        for x in range(lo, hi + 1):
            if is_prime(x):
                local_primes.append(x)

        with bb_lock:
            bb["results"].extend(local_primes)
            bb["completed_tasks"] += 1


def build_tasks(start: int, end: int, chunk_size: int) -> List[Tuple[int, int]]:
    tasks: List[Tuple[int, int]] = []
    cur = start
    while cur <= end:
        tasks.append((cur, min(cur + chunk_size - 1, end)))
        cur += chunk_size
    return tasks


def main() -> None:
    start, end = 1, 200_000
    chunk_size = 2_000
    num_workers = 8

    manager = Manager()
    bb = manager.dict()
    bb_lock = Lock()

    bb["tasks"] = manager.list(build_tasks(start, end, chunk_size))
    bb["results"] = manager.list()
    bb["done"] = False
    bb["completed_tasks"] = 0

    procs: List[Process] = []
    t0 = time.time()

    for i in range(num_workers):
        p = Process(target=worker_blackboard, args=(bb, bb_lock, i))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    primes = sorted(list(bb["results"]))
    dt = time.time() - t0

    print(f"Found {len(primes)} primes in [{start}, {end}] using Blackboard in {dt:.2f}s")
    print(f"Completed tasks: {bb['completed_tasks']}")
    print("First 20:", primes[:20])
    print("Last  20:", primes[-20:])


if __name__ == "__main__":
    main()
