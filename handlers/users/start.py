from aiogram import types
from aiogram.dispatcher.filters.builtin import CommandStart
from aiogram.types import ReplyKeyboardRemove

from keyboards.default import add_feed
from loader import dp, db


@dp.message_handler(CommandStart())
async def bot_start(message: types.Message):
    await message.answer(f"Привет, {message.from_user.full_name}!", reply_markup=ReplyKeyboardRemove())
    if not db.select_user(user_id=message.from_user.id):
        await message.answer("Добавляем тебя в БД...")
        db.add_user(message.from_user.id)
    await message.answer("Готов к работе!", reply_markup=add_feed)
