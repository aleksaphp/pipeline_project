import asyncio
import logging
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


if __name__ == "__main__":
    logger = create_logger()
    run_pipeline(logger)
