import logging
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
from threading import Event, Lock, Thread
from time import sleep

lock = Lock()
stats = {"saved": 0}  # безопасный счетчик успешно сохраненных постов
logger = logging.getLogger("app")


def worker(queue: Queue, stop_event: Event) -> None:
    while not stop_event.is_set():
        try:
            post = queue.get(timeout=0.5)
        except Empty:
            continue

        try:
            save_post(post)
        except Exception:
            logger.exception("Не удалось сохранить пост %r", post)
        finally:
            queue.task_done()


def save_post(post: dict) -> None:
    with lock:
        logger.info(
            f"Пост {post['id']}, успешно обработан и записан в файл (title: {post['title']}, hash: {post['text_hash']})"
        )
        stats["saved"] += 1
    sleep(0.05)


def print_progress(stop_event: Event, queue_size: int) -> None:
    while not stop_event.is_set():
        print(f">>> Прогресс сохранения: {stats['saved']} / {queue_size}")
        if stop_event.wait(2):
            break
    print(f">>> Прогресс сохранения: {stats['saved']} / {queue_size}")


def save_posts(queue: Queue, workers: int = 5) -> int:
    stats["saved"] = 0  # сбрасываем, чтобы избежать ошибок
    stop_event = Event()

    progress_thread = Thread(
        target=print_progress, args=(stop_event, queue.qsize()), daemon=True
    )
    progress_thread.start()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        [executor.submit(worker, queue, stop_event) for _ in range(workers)]
        try:
            queue.join()
        finally:
            stop_event.set()
            progress_thread.join(1)

    return stats["saved"]
