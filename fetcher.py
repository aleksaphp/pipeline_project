import asyncio
import json
import traceback
from logging import ERROR, INFO, Logger, getLogger

import aiohttp

API_URL = "https://jsonplaceholder.typicode.com/posts/{post_id}"


async def log(logger: Logger, message: str, level: int = INFO) -> None:
    logger.log(level, message)


async def fetch_post(
    session: aiohttp.ClientSession,
    post_id: int,
    semaphore: asyncio.Semaphore,
    log_tasks: list[asyncio.Task],
    logger: Logger,
) -> dict | None:
    async with semaphore:
        try:
            log_tasks.append(
                asyncio.create_task(log(logger, f"Начало загрузки поста {post_id}..."))
            )
            async with session.get(API_URL.format(post_id=post_id)) as response:
                response.raise_for_status()
                post = await response.json()

            log_tasks.append(
                asyncio.create_task(log(logger, f"Пост {post_id} успешно загружен"))
            )
            return post
        except (TimeoutError, aiohttp.ClientError, json.JSONDecodeError):
            log_tasks.append(
                asyncio.create_task(
                    log(
                        logger,
                        f"Не удалось загрузить пост {post_id}, error_text: {traceback.format_exc()}",
                        ERROR,
                    )
                )
            )
            return None


async def fetch_all_posts(posts_amount=100, timeout=10) -> list[dict]:
    semaphore = asyncio.Semaphore(5)
    logger = getLogger("app")

    tasks = []
    log_tasks = []
    post_ids = list(range(1, posts_amount + 1))

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=timeout)
    ) as session:
        for post_id in post_ids:
            tasks.append(
                asyncio.create_task(
                    fetch_post(session, post_id, semaphore, log_tasks, logger)
                )
            )

        try:
            results = await asyncio.gather(*tasks)
            successful_results = [post for post in results if post is not None]
        finally:
            await asyncio.gather(*log_tasks, return_exceptions=True)

    return successful_results
