import asyncio
from logging import Logger, INFO, ERROR

import aiohttp

API_URL = "https://jsonplaceholder.typicode.com/posts/{post_id}"


async def log(logger: Logger, message: str, level: int = INFO) -> None:
    logger.log(level, message)


async def fetch_post(session: aiohttp.ClientSession, post_id: int, semaphore: asyncio.Semaphore, logger: Logger) -> dict:
    async with semaphore:
        asyncio.create_task(log(logger, f"Начало загрузки поста {post_id}..."))
        async with session.get(f"https://jsonplaceholder.typicode.com/posts/{post_id}") as response:
            response.raise_for_status()
            post = await response.json()

        asyncio.create_task(log(logger, f"Пост {post_id} успешно загружен"))
        return post


async def fetch_all_posts(logger: Logger, posts_amount: int, timeout=10) -> list[dict]:
    semaphore = asyncio.Semaphore(5)

    tasks = []
    post_ids = list(range(1, posts_amount + 1))

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        for post_id in post_ids:
            tasks.append(asyncio.create_task(fetch_post(session, post_id, semaphore, logger)))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        successful_results = []

        for post_id, result in zip(post_ids, results):
            if isinstance(result, BaseException):
                asyncio.create_task(log(logger, f"Пост {post_id} не удалось загрузить: "
                                                f"{type(result).__name__}: {result}", ERROR))
            else:
                successful_results.append(result)

    return successful_results
