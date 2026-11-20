import asyncio

import aiohttp
from aiologger import Logger

API_URL = "https://jsonplaceholder.typicode.com/posts/{post_id}"


async def fetch_post(session: aiohttp.ClientSession, post_id: int, semaphore: asyncio.Semaphore, logger: Logger) -> dict:
    """
    Все же решил использовать logger.info без await, поскольку такой вызов несет тот же функционал вызова
    фоновой функции логирования, как и с asyncio.create_task, а обернуть asyncio.create_task(logger.info())
    нельзя, потому что logger.info() возвращает Task, а не Coroutine
    Вариант использования я асинхронного логгера здесь мне кажется уместнее
    """
    async with semaphore:
        logger.info(f"Начало загрузки поста {post_id}...")
        async with session.get(f"https://jsonplaceholder.typicode.com/posts/{post_id}") as response:
            response.raise_for_status()
            post = await response.json()

        logger.info(f"Пост {post_id} успешно загружен")
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
                logger.error(
                     f"Пост {post_id} не удалось загрузить: "
                     f"{type(result).__name__}: {result}"
                )
            else:
                successful_results.append(result)

    await logger.shutdown() # гарантируем, что все фоновые записи в лог завершаться
    return successful_results
