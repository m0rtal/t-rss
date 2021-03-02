from aiogram.dispatcher.filters.state import StatesGroup, State


class FullPath(StatesGroup):
    awaiting_feed = State()
