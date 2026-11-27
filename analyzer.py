import math
import multiprocessing as mp
import os
import queue
from concurrent.futures import ProcessPoolExecutor
from hashlib import sha256
from typing import Any


def monitor_process(q: queue.Queue):
    while True:
        try:
            post_id = q.get(timeout=0.1)
            if post_id is None:
                break
            print(f"[Monitor] Пост {post_id} обработан.")
        except queue.Empty:
            continue


def process_single_post(
    post: dict, q: mp.Queue, counter: Any, lock: Any
) -> dict[str, Any]:
    complexity_score = sum(math.sin(i) for i in range(1_000_000)) * len(post["body"])
    text_hash = sha256((post["title"] + post["body"]).encode("utf-8")).hexdigest()

    post["complexity_score"] = complexity_score
    post["text_hash"] = text_hash

    q.put(post["id"])

    with lock:
        counter.value += 1
    return post


def process_posts(posts: list[dict[str, Any]]) -> tuple[queue.Queue, int]:
    with mp.Manager() as manager:
        q = manager.Queue()
        counter = manager.Value("i", 0)
        lock = manager.Lock()

        monitor = mp.Process(target=monitor_process, args=(q,))
        monitor.start()

        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [
                executor.submit(process_single_post, post, q, counter, lock)
                for post in posts
            ]

            resulted_posts = [f.result() for f in futures]

        q.put(None)  # Сигнал монитору, что все посты обработаны
        monitor.join()

        threading_Queue = queue.Queue()
        for post in resulted_posts:
            threading_Queue.put(post)

        return threading_Queue, counter.value
