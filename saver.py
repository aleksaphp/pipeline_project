import traceback
from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from queue import Queue, Empty
from threading import Lock, Event, Thread
from time import sleep


def worker(queue: Queue, stop_event: Event, lock: Lock, stats: dict[str, int], logger: Logger) -> None:
    while not stop_event.is_set():
        try:
            post = queue.get(timeout=0.5)
        except Empty:
            continue

        try:
            save_post(post, lock, stats, logger)
        except Exception:
            logger.error(
                "Не удалось сохранить пост %s, error_text: %s",
                post.get("id"), traceback.format_exc(),
            )
        finally:
            queue.task_done()


def save_post(post: dict, lock: Lock, stats: dict[str, int], logger: Logger) -> None:
    with lock:
        logger.info(
            f"Пост {post['id']}, успешно обработан и записан в файл (title: {post['title']}, hash: {post['text_hash']})")  # записываем какую-то инфу о посте в results.log
        stats["saved"] += 1
    sleep(0.05)


def print_progress(stats: dict[str, int], stop_event: Event, queue_size: int) -> None:
    while not stop_event.is_set():
        print(f">>> Прогресс сохранения: {stats["saved"]} / {queue_size}")
        if stop_event.wait(2):
            break


def save_posts(queue: Queue, logger: Logger, workers: int = 5) -> int:
    lock = Lock()
    stop_event = Event()
    stats = {"saved": 0}  # безопасный счетчик успешно сохраненных постов

    progress_thread = Thread(target=print_progress, args=(stats, stop_event, queue.qsize()), daemon=True)
    progress_thread.start()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        [executor.submit(worker, queue, stop_event, lock, stats, logger) for _ in range(workers)]

        queue.join()
        stop_event.set()
        progress_thread.join(1)

    return stats["saved"]
