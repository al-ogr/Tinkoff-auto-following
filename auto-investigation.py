'''Автоматическое повторение сделок у брокера Т-Банк'''
import os
from dotenv import load_dotenv
from pandas import DataFrame
import time

from tinkoff.invest import Client, SecurityTradingStatus
from tinkoff.invest.services import InstrumentsService
from tinkoff.invest.utils import quotation_to_decimal


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
# Получение токенов из конфигурационного файла
TOKEN_SOURCE = os.environ["TOKEN_SOURCE"]
TOKEN_TARGET = os.environ["TOKEN_TARGET"]

def main():
    # Подключение к счетам
    with Client(TOKEN_SOURCE) as client_source, Client(TOKEN_TARGET) as client_target:
        # Получение ссылок на счета
        account_source = client_source.users.get_accounts().accounts[0]
        account_target = client_target.users.get_accounts().accounts[0]
        # Получение словаря инструментов figi-name
        instruments: InstrumentsService = client_target.instruments
        tickers = []
        for method in ["shares", "bonds", "etfs", "currencies", "futures"]:
            for item in getattr(instruments, method)().instruments:
                tickers.append(
                    {
                        "name": item.name,
                        "ticker": item.ticker,
                        "class_code": item.class_code,
                        "figi": item.figi,
                        "uid": item.uid,
                        "type": method,
                        "min_price_increment": quotation_to_decimal(
                            item.min_price_increment
                        ),
                        "scale": 9 - len(str(item.min_price_increment.nano)) + 1,
                        "lot": item.lot,
                        "trading_status": str(
                            SecurityTradingStatus(item.trading_status).name
                        ),
                        "api_trade_available_flag": item.api_trade_available_flag,
                        "currency": item.currency,
                        "exchange": item.exchange,
                        "buy_available_flag": item.buy_available_flag,
                        "sell_available_flag": item.sell_available_flag,
                        "short_enabled_flag": item.short_enabled_flag,
                        "klong": quotation_to_decimal(item.klong),
                        "kshort": quotation_to_decimal(item.kshort),
                    }
                )
        dict_instr = DataFrame(tickers)
        dict_instr = dict_instr.set_index('figi')
            
        while True:
            positions = client_source.operations.get_positions(account_id=account_source.id)
            # Очистка экрана
            os.system('cls') # Windows 
            #os.system('clear') # Linux
            # Акции, фонды
            for pos in positions.securities:
                print(f"{dict_instr['name'].get(pos.figi, pos.figi)} - {pos.balance}")
            # Фьючерсы
            for pos in positions.futures:
                print(f"{dict_instr['name'].get(pos.figi, pos.figi)} - {pos.balance}")
            # Валюта
            for pos in positions.money:
                print(f"{dict_instr['name'].get(pos.currency, pos.currency)} - {pos.units + pos.nano/1e9}")
            
            # Ожидание
            time.sleep(1)


if __name__ == "__main__":
    main()