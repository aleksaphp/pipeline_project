import asyncio

from aiologger import Logger
from aiologger.formatters.base import Formatter
from aiologger.handlers.files import AsyncFileHandler
from aiologger.levels import LogLevel

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

async def run_pipeline():
    results = await fetch_all_posts(create_logger())


def main():
    asyncio.run(run_pipeline())


if __name__ == '__main__':
    main()