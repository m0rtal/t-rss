import asyncio

import feedparser
from aiogram import executor

from loader import dp, db, bot
import middlewares, filters, handlers
from utils import set_default_commands
from utils.notify_admins import on_startup_notify
from datetime import datetime
from random import randint


async def on_startup(dispatcher):
    # Уведомляет про запуск
    await on_startup_notify(dispatcher)
    await set_default_commands(dispatcher)


def get_rss_links(rss):
    feed = feedparser.parse(rss)

    if feed.entries:
        links = [(entry.link, entry.description) for entry in feed.entries]
        return tuple(links[::-1])
    else:
        return None


async def get_rss_updates(wait_for):
    while True:
        for user in db.select_users():
            known_links = db.select_links(user_id=user)
            for feed in db.select_user_subscriptions(user_id=user):
                rss_links = get_rss_links(feed)
                for link in rss_links:
                    if link[0] not in known_links:
                        db.add_link(feed=feed, link=link[0])
                        db.add_description(link=link[0], description=link[1].replace("<br />", "\n"))
        await asyncio.sleep(wait_for)


async def send_news(wait_for):
    while True:
        for user in db.select_users():
            for_send = db.get_unsent_posts(user_id=user)
            for post in for_send:
                await bot.send_message(user, text=f"{post[1]}\n\n<a href='{post[0]}'>Источник</a>")
                db.mark_sended(user_id=user, link=post[0])
                await asyncio.sleep(randint(1, 5))
        await asyncio.sleep(wait_for)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(get_rss_updates(60 * 10))
    loop.create_task(send_news(60 * 15))
    executor.start_polling(dp, on_startup=on_startup)
