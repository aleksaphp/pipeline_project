import math
import os
from concurrent.futures import ProcessPoolExecutor
from hashlib import sha256
from multiprocessing import Process, Queue, Manager
from queue import Empty
from typing import Any


def monitor_process(q: Queue):
    while True:
        try:
            post_id = q.get(timeout=.1)
            if post_id is None:
                break
            print(f"[Monitor] Пост {post_id} обработан.")
        except Empty:
            continue


def process_single_post(post: dict, q: Queue, counter: Any, lock: Any) -> dict[str, Any]:
    complexity_score = sum(math.sin(i) for i in range(1_000_000)) * len(post["body"])
    text_hash = sha256((post["title"] + post["body"]).encode('utf-8')).hexdigest()

    post["complexity_score"] = complexity_score
    post["text_hash"] = text_hash

    q.put(post["id"])

    with lock:
        counter.value += 1
    return post


def process_posts(posts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    with Manager() as manager:
        q = manager.Queue()
        counter = manager.Value('i', 0)
        lock = manager.Lock()

        monitor = Process(target=monitor_process, args=(q,))
        monitor.start()

        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [executor.submit(process_single_post, post, q, counter, lock) for post in posts]

            results = [f.result() for f in futures]

        q.put(None)  # Сигнал монитору, что все посты обработаны
        monitor.join()

        return results, counter.value
