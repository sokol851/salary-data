import asyncio
import logging

from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from decouple import config
from fastapi import FastAPI, Query
from motor.motor_asyncio import AsyncIOMotorClient

from .agregator import aggregate_salaries

# Включение логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = config('TELEGRAM_TOKEN')

# Создание приложения FastAPI
app = FastAPI()

# Создание бота
bot = Bot(token=TOKEN)
dp = Dispatcher()

# Подключение к MongoDB через Motor
client = AsyncIOMotorClient(config('HOST'))
db = client.company_db
collection = db.salaries


@dp.message(Command(commands=['start']))
async def send_welcome(message: types.Message):
    """ Приветствие """
    await message.reply(
        "Привет! Я могу помочь с агрегацией зарплат.\n"
        "Используйте команду /aggregate.\n\n"
        "Формат: /aggregate <dt_from> <dt_upto> <group_type>\n"
        "Пример: /aggregate 2022-12-31T00:00:00 2022-12-31T23:59:00 hour"
    )


@dp.message(Command(commands=['aggregate']))
async def handle_aggregate(message: types.Message):
    """ Агрегация данных """
    try:
        _, dt_from, dt_upto, group_type = message.text.split()
        result = await aggregate_salaries(dt_from,
                                          dt_upto,
                                          group_type,
                                          collection)
        response = (
            "Aggregated salaries:\n"
            f"Labels: {result['labels']}\n"
            f"Dataset: {result['dataset']}"
        )
        await message.reply(response, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await message.reply(
            "Ошибка в агрегации. Использование: /aggregate "
            "<dt_from> <dt_upto> <group_type>"
        )
        logger.error(f"Ошибка в агрегации: {e}")


@app.on_event("startup")
async def on_startup():
    """ Запуск Aiogram в фоне """
    asyncio.create_task(dp.start_polling(bot))


@app.get(
    "/items/",
    summary="Получение агрегации данных.",
    description="Возвращает агрегацию данных на основе предоставленных "
                "временных рамок и интервала."
)
async def read_salary(
        dt_from: str = Query(
            description="Начальная дата и время в формате ISO 8601."
                        " Пример: 2022-12-31T00:00:00"),
        dt_upto: str = Query(
            description="Конечная дата и время в формате ISO 8601."
                        " Пример: 2022-12-31T23:59:00"),
        group_type: str = Query(
            description="Временной интервал: hour, day, month, year.")
) -> dict[str, list]:
    """
    Получение агрегации данных

    - **dt_from**: Начальная дата и время для агрегации.
    - **dt_upto**: Конечная дата и время для агрегации.
    - **group_type**: Временной интервал: hour, day, month, year.
    """
    result = await aggregate_salaries(dt_from, dt_upto, group_type, collection)
    return result
