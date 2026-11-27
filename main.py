import asyncio
import logging
import queue
from time import perf_counter

from analyzer import process_posts
from fetcher import fetch_all_posts
from saver import save_posts


def create_logger() -> logging.Logger:
    logger = logging.getLogger("app")
    logger.setLevel(logging.INFO)

    if getattr(logger, "_app_configured", False):
        return logger

    fh = logging.FileHandler("results.log", mode="a", encoding="utf-8")
    fh.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(fh)
    logger.propagate = False

    logger._app_configured = True  # маркер, что logger создан
    return logger


def run_pipeline(logger: logging.Logger, posts_amount=100):
    start = perf_counter()
    downloaded_results = asyncio.run(fetch_all_posts())
    processed_posts = process_posts(downloaded_results)
    q = queue.Queue()
    for post in processed_posts:
        q.put(post)
    threading_counter = save_posts(q, logger)

    print("=== Отчёт о работе пайплайна ===")
    print(f"Запрошено постов: {posts_amount}")
    print(f"Успешно загружено: {len(downloaded_results)}")
    print(f"Успешно обработано (CPU): {len(processed_posts)}")
    print(f"Успешно сохранено (I/O): {threading_counter}")
    print(f"Общее время: {perf_counter() - start:.2f} сек.")


if __name__ == "__main__":
    logger = create_logger()
    run_pipeline(logger)
