import asyncio
import logging

import requests
from decouple import config
from fastapi import FastAPI

from .agregator import aggregate_salaries

app = FastAPI()

# Включение логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Получение токена из конфигурации
TOKEN = config('TELEGRAM_TOKEN')
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"


def send_message(chat_id, text):
    """ Метод отправки сообщения для телеграм """
    url = f"{BASE_URL}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text
    }
    requests.post(url, json=payload)


async def poll_updates():
    offset = 0
    while True:
        url = f"{BASE_URL}/getUpdates"
        payload = {"offset": offset, "timeout": 10}
        response = requests.get(url, params=payload)
        updates = response.json().get("result", [])

        for update in updates:
            offset = update['update_id'] + 1
            if "message" in update:
                chat_id = update["message"]["chat"]["id"]
                text = update["message"].get("text", "")

                if text.startswith("/start"):
                    send_message(
                        chat_id,
                        ("Привет! Я могу помочь с агрегацией зарплат.\n"
                         "Используйте команду /aggregate.\n\n"
                         "Формат: /aggregate "
                         "<dt_from> <dt_upto> <group_type>\n"
                         "Пример: /aggregate "
                         "2022-12-31T00:00:00 2022-12-31T23:59:00 hour")
                    )

                elif text.startswith("/aggregate"):
                    try:
                        _, dt_from, dt_upto, group_type = text.split()
                        result = aggregate_salaries(
                            dt_from, dt_upto, group_type
                        )
                        response = (
                            "Aggregated salaries:\n"
                            f"Labels: {result['labels']}\n"
                            f"Dataset: {result['dataset']}"
                        )
                        send_message(chat_id, response)
                    except Exception as e:
                        send_message(
                            chat_id,
                            ("Ошибка в агрегации. Использование: /aggregate "
                             "<dt_from> <dt_upto> <group_type>")
                        )
                        logger.error(f"Ошибка в агрегации: {e}")

                if text.startswith('/hi'):
                    send_message(chat_id, 'Привет!')

        await asyncio.sleep(1)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(poll_updates())
