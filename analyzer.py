import logging
import math
import multiprocessing as mp
import multiprocessing.sharedctypes as mp_shared
import multiprocessing.synchronize as mp_sync
import os
from concurrent.futures import ProcessPoolExecutor
from hashlib import sha256
from typing import Any

worker_lock = None
worker_counter = None


def init_worker(counter: mp_shared.Synchronized[int], lock: mp_sync.Lock):
    global worker_counter, worker_lock
    worker_counter = counter
    worker_lock = lock


def monitor_process(q: mp.Queue):
    while True:
        post_id = q.get()  # без timeout
        if post_id is None:
            break
        print(f"[Monitor] Пост {post_id} обработан.")


def process_single_post(post: dict) -> dict[str, Any] | None:
    try:
        complexity_score = sum(math.sin(i) for i in range(1_000_000)) * len(
            post["body"]
        )
        text_hash = sha256((post["title"] + post["body"]).encode("utf-8")).hexdigest()

        post["complexity_score"] = complexity_score
        post["text_hash"] = text_hash

        with worker_lock:  # type: ignore[union-attr]
            worker_counter.value += 1  # type: ignore[union-attr]
        return post
    except (KeyError, TypeError):
        logger = logging.getLogger("app")
        logger.exception(f"Не удалось обработать пост {post['id']}")


def process_posts(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    q = mp.Queue()
    counter = mp.Value("i", 0)
    lock = mp.Lock()

    monitor = mp.Process(target=monitor_process, args=(q,))
    monitor.start()
    try:
        with ProcessPoolExecutor(
            max_workers=os.cpu_count(),
            initializer=init_worker,
            initargs=(counter, lock),
        ) as executor:
            futures = [executor.submit(process_single_post, post) for post in posts]
            resulted_posts = []
            for f in futures:
                resulted_post = f.result()
                if resulted_post is not None:
                    resulted_posts.append(resulted_post)
                    q.put(resulted_post["id"])
    finally:
        q.put(None)  # Сигнал монитору, что все посты обработаны
        q.close()
        monitor.join()

    return resulted_posts
