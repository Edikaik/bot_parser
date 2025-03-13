import os
import requests
from lxml import html
import pandas as pd
import sqlite3
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import asyncio
from dotenv import load_dotenv

load_dotenv()


class PriceParser:

    def __init__(self):
        token = os.getenv("TELEGRAM_TOKEN")
        if not token:
            raise ValueError("Переменная окружения TELEGRAM_TOKEN не задана.")
        self.token = token
        self.bot = Bot(token=token)
        self.dispatcher = Dispatcher()

    async def start_bot(self):
        await self.bot.delete_webhook(drop_pending_updates=True)
        await self.dispatcher.start_polling(self.bot)

    async def handle_start(self, message: types.Message):
        await message.answer("Привет! Загрузите файл Excel с данными.")

    async def handle_file(self, message: types.Message):
        file = await self.bot.download(message.document.file_id)
        with open("uploaded_file.xlsx", "wb") as f:
            f.write(file.read())
        data = pd.read_excel("uploaded_file.xlsx")
        response_text = self.process_and_save_to_db(data)
        await message.answer(response_text)

    def fetch_price(self, url: str, xpath: str) -> str:
        try:
            response = requests.get(url)
            response.raise_for_status()
            tree = html.fromstring(response.content)
            price_elements = tree.xpath(xpath)
            if price_elements:
                raw_price = price_elements[0].text_content()
                cleaned_price = " ".join(raw_price.split()).strip()
                return cleaned_price
            return None
        except Exception as e:
            print(f"Ошибка при извлечении цены: {e}")
            return None

    def process_and_save_to_db(self, data: pd.DataFrame) -> str:
        conn = sqlite3.connect("sites.db")
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sites (
                title TEXT,
                url TEXT,
                price TEXT
            )
        """
        )
        response_lines = []
        for _, row in data.iterrows():
            price = self.fetch_price(row["url"], row["xpath"])
            cursor.execute(
                "INSERT INTO sites (title, url, price) VALUES (?, ?, ?)",
                (row["title"], row["url"], price),
            )
            response_lines.append(
                f"Товар: {row['title']}\nЦена: {price or 'Не удалось извлечь'}\nURL: {row['url']}"
            )
        conn.commit()
        conn.close()
        return "\n\n".join(response_lines)

    def setup_handlers(self):
        self.dispatcher.message(Command(commands=["start"]))(self.handle_start)
        self.dispatcher.message(
            lambda message: message.document
            and message.document.mime_type
            == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )(self.handle_file)


if __name__ == "__main__":
    parser = PriceParser()
    parser.setup_handlers()
    asyncio.run(parser.start_bot())
