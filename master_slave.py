from __future__ import annotations
import math
from multiprocessing import Pool, cpu_count
from dataclasses import dataclass
from typing import List, Tuple
import time


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


def primes_in_range(lo: int, hi: int) -> List[int]:
    out: List[int] = []
    for x in range(lo, hi + 1):
        if is_prime(x):
            out.append(x)
    return out


@dataclass(frozen=True)
class Task:
    lo: int
    hi: int


def chunk_ranges(start: int, end: int, chunk_size: int) -> List[Task]:
    tasks: List[Task] = []
    cur = start
    while cur <= end:
        tasks.append(Task(cur, min(cur + chunk_size - 1, end)))
        cur += chunk_size
    return tasks


def main() -> None:
    start, end = 1, 200_000
    chunk_size = 10_000

    tasks = chunk_ranges(start, end, chunk_size)
    
    t0 = time.time()

    with Pool(processes=cpu_count()) as pool:
        results: List[List[int]] = pool.starmap(primes_in_range, [(t.lo, t.hi) for t in tasks])

    primes = [p for sub in results for p in sub]
    dt = time.time() - t0

    print(f"Found {len(primes)} primes in [{start}, {end}] using Blackboard in {dt:.2f}s")

    print(f"Found {len(primes)} primes in [{start}, {end}]")
    print("First 20:", primes[:20])
    print("Last  20:", primes[-20:])


if __name__ == "__main__":
    main()
