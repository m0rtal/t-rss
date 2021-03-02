from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

add_feed = ReplyKeyboardMarkup(
    [
        [
            KeyboardButton("Добавить RSS фид")
        ],
    ],
    resize_keyboard=True
)