import asyncio

from aiologger import Logger
from aiologger.formatters.base import Formatter
from aiologger.handlers.files import AsyncFileHandler
from aiologger.levels import LogLevel

from analyzer import process_posts
from fetcher import fetch_all_posts


def create_logger() -> Logger:
    logger = Logger(
        name="app",
        level=LogLevel.INFO,
    )

    file_handler = AsyncFileHandler(
        filename="results.log",
        mode="a",
        encoding="utf-8",
    )

    file_handler.formatter = Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.add_handler(file_handler)

    return logger


def run_pipeline(posts_amount=100):
    downloaded_results = asyncio.run(fetch_all_posts(create_logger(), posts_amount))
    processed_results, processed_counter = process_posts(downloaded_results)


if __name__ == '__main__':
    run_pipeline()
