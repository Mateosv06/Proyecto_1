# flow.py
from prefect import flow, task
import yfinance as yf
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import SMAIndicator
from telegram import Bot
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TICKER


@task
def get_data(ticker: str):
    # ✅ Ajuste: periodo e intervalo compatibles con datos horarios
    data = yf.download(ticker, period="60d", interval="1h")

    # Validación para evitar el error "No objects to concatenate"
    if data.empty:
        raise ValueError(f"No se obtuvieron datos para el ticker {ticker}.")
    return data


@task
def calculate_indicators(data: pd.DataFrame):
    data["RSI"] = RSIIndicator(data["Close"], window=14).rsi()
    data["MA200"] = SMAIndicator(data["Close"], window=200).sma_indicator()
    return data


@task
def analyze(data: pd.DataFrame):
    last_row = data.iloc[-1]
    rsi = round(last_row["RSI"], 2)
    close = last_row["Close"]
    ma200 = last_row["MA200"]
    distance = round(((close - ma200) / ma200) * 100, 2)
    return rsi, distance, close, ma200


@task
def send_telegram_message(rsi, distance, close, ma200):
    bot = Bot(token=TELEGRAM_TOKEN)
    message = (
        f"📊 *{TICKER}* Update:\n"
        f"Precio actual: ${close:.2f}\n"
        f"RSI: {rsi}\n"
        f"Distancia a MA200: {distance}%\n"
        f"MA200: ${ma200:.2f}\n"
    )
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode="Markdown")


@flow
def rsi_monitor():
    data = get_data(TICKER)
    data = calculate_indicators(data)
    rsi, distance, close, ma200 = analyze(data)
    send_telegram_message(rsi, distance, close, ma200)


if __name__ == "__main__":
    rsi_monitor()

