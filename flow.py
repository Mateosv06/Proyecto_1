from prefect import flow, task
import yfinance as yf
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import SMAIndicator
from telegram import Bot
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TICKER
import time

# ==================== TAREAS ====================

@task
def get_data(ticker: str):
    print(f"📥 Descargando datos para {ticker}...")
    data = yf.download(ticker, period="60d", interval="1h")

    # Si tiene columnas multinivel (por ejemplo ('Close','AAPL')), las aplanamos
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0] for col in data.columns]

    # Convertir todo a float
    data = data.astype(float)
    return data


@task
def calculate_indicators(data: pd.DataFrame):
    print("📊 Calculando RSI y medias móviles...")
    close_series = data["Close"].squeeze()  # Garantiza que sea Serie 1D

    data["RSI"] = RSIIndicator(close_series, window=14).rsi()
    data["MA200"] = SMAIndicator(close_series, window=200).sma_indicator()
    return data


@task
def check_signals(data: pd.DataFrame):
    print("🔍 Revisando señales de trading...")

    last_rsi = float(data["RSI"].iloc[-1])
    last_price = float(data["Close"].iloc[-1])
    ma200 = float(data["MA200"].iloc[-1])

    print(f"Último RSI: {last_rsi:.2f} | Precio actual: {last_price:.2f}")

    if last_rsi < 30 and last_price > ma200:
        signal = "🟢 Posible *compra* (RSI < 30 y precio > MA200)"
    elif last_rsi > 70 and last_price < ma200:
        signal = "🔴 Posible *venta* (RSI > 70 y precio < MA200)"
    else:
        signal = "⚪ Sin señal clara"
    return signal, last_rsi, last_price, ma200


@task
@task
def send_telegram_message(signal, rsi, price, ma200):
    from telegram import Bot
    import asyncio

    async def main():
        bot = Bot(token=TELEGRAM_TOKEN)
        message = (
            f"📊 *{TICKER}* Update:\n"
            f"Precio actual: ${price:.2f}\n"
            f"RSI: {rsi:.2f}\n"
            f"MA200: ${ma200:.2f}\n"
            f"Señal: {signal}"
        )
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode="Markdown")

    try:
        asyncio.run(main())
        print("✅ Mensaje enviado a Telegram.")
    except Exception as e:
        print(f"⚠️ Error enviando mensaje a Telegram: {e}")



# ==================== FLUJO PRINCIPAL ====================

@flow
def rsi_monitor():
    print("🚀 Iniciando monitoreo RSI...")
    try:
        data = get_data(TICKER)
        data = calculate_indicators(data)
        signal, rsi, price, ma200 = check_signals(data)
        send_telegram_message(signal, rsi, price, ma200)
    except Exception as e:
        print(f"❌ Error en flujo RSI: {e}")


# ==================== LOOP AUTOMÁTICO ====================

if __name__ == "__main__":
    while True:
        rsi_monitor()
        print("⏱ Esperando 1 hora para próxima ejecución...\n")
        time.sleep(3600)
