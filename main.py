import asyncio
import logging
from time import perf_counter

from analyzer import process_posts
from fetcher import fetch_all_posts
from saver import save_posts


def create_logger() -> logging.Logger:
    logger = logging.getLogger("app")
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(
        filename="results.log",
        mode="a",
        encoding="utf-8",
    )

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger


def run_pipeline(posts_amount=100):
    start = perf_counter()
    logger = create_logger()
    downloaded_results = asyncio.run(fetch_all_posts(logger, posts_amount))
    threading_queue, processed_counter = process_posts(downloaded_results)
    threading_counter = save_posts(threading_queue, logger)

    print(f"""
    === Отчёт о работе пайплайна ===
    Запрошено постов: {posts_amount}
    Успешно загружено: {len(downloaded_results)}
    Успешно обработано (CPU): {processed_counter}
    Успешно сохранено (I/O): {threading_counter}
    Общее время: {perf_counter() - start:.2f} сек.
    """)


if __name__ == '__main__':
    run_pipeline()
