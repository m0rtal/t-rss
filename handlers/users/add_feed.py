from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.types import ReplyKeyboardRemove

from loader import dp, db
from states.full_path import FullPath


@dp.message_handler(text="Добавить RSS фид", state="*")
async def add_feed(message: types.Message):
    await message.answer("Жду RSS фид:", reply_markup=ReplyKeyboardRemove())
    await FullPath.awaiting_feed.set()


@dp.message_handler(state=FullPath.awaiting_feed)
async def add_feed(message: types.Message, state=FSMContext):
    db.add_subscription(user_id=message.from_user.id, feed=message.text)
    await message.answer("Добавил!")
    await state.finish()
