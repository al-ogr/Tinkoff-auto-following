'''Автоматическое повторение сделок у брокера Т-Банк'''
import os
from dotenv import load_dotenv
import pandas as pd
import time
import platform
import numpy as np
import math

from tinkoff.invest import Client, SecurityTradingStatus, Account
from tinkoff.invest import OrderExecutionReportStatus, OrderType, OrderDirection
from tinkoff.invest.services import InstrumentsService
from tinkoff.invest.utils import quotation_to_decimal


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
# Получение токенов из конфигурационного файла
TOKEN_SOURCE = os.environ["TOKEN_SOURCE"]
TOKEN_TARGET = os.environ["TOKEN_TARGET"]
# Период обновления сравнения
period_reload = int(os.environ["period_reload"])
# Коэффициент пересчета размера позиций
ratio_account = float(os.environ["ratio_account"])
# Определение команды для очистки экрана
clr_command = 'cls' if platform.system() == 'Windows' else 'clear'

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
        df_dict_instr = pd.DataFrame(tickers)
        df_dict_instr = df_dict_instr.set_index('figi')
        
        # Инициализация DataFrame для исходного и целевого счетов
        df_account_source = pd.DataFrame(columns=['id', 'Наименование', 'Количество', 'Тип актива'])
        df_account_target = pd.DataFrame(columns=['id', 'Наименование', 'Количество', 'Тип актива'])
        # Маркер отображения
        was_printing = False
            
        while True:
            # Очистка DataFrame
            df_account_source.drop(df_account_source.index, inplace=True)
            df_account_target.drop(df_account_source.index, inplace=True)
            # Заполнение текущими данными
            df_account_source = position_to_dataframe(client_source, 
                                                      account_source,
                                                      df_dict_instr)
            df_account_target = position_to_dataframe(client_target, 
                                                      account_target,
                                                      df_dict_instr)
            # Сравнение исходного и целевого счетов и вычисление разницы
            df_for_buy, df_for_sell = get_account_difference(df_account_source,
                                                             df_account_target,
                                                             ratio_account,
                                                             df_dict_instr)
            # Выполнение заданий на покупку/продажу по рынку
            df_not_sell = start_deal_tasks(client_target,
                                           account_target,
                                           df_for_sell,
                                           False)
            # Выполнение заданий на покупку/продажу по рынку
            df_not_buy = start_deal_tasks(client_target,
                                          account_target,
                                          df_for_buy,
                                          True)
            # Если есть невыполненные задания на покупку/продажу - вывод на экран
            if not was_printing \
               or (df_not_buy.shape[0] > 0 or df_not_sell.shape[0] > 0) \
               or (df_for_buy.shape[0] > 0 or df_for_sell.shape[0] > 0):
                was_printing = True
                # Очистка экрана
                os.system(clr_command)
                print('Исходный', '='*70)
                print(df_account_source.sort_values(by=['Тип актива', 'Наименование'], ignore_index=True))
                print('Целевой', '='*72)
                print(df_account_target.sort_values(by=['Тип актива', 'Наименование'], ignore_index=True))
                print('Невыполненные задания на покупку', '='*47)
                print(df_not_buy if not df_not_buy.empty else 'отсутствуют')
                print('Невыполненные задания на продажу', '='*47)
                print(df_not_sell if not df_not_sell.empty else 'отсутствуют')
            # Ожидание
            time.sleep(period_reload)


def position_to_dataframe(client: Client, 
                          account: Account, 
                          df_dict_instr: pd.DataFrame) -> pd.DataFrame:
    ''' Функция получения списка открытых позиций, данные возвращаются 
        в формате pandas.DataFrame.
       
       Args:
        client (tinkoff.invest.Client):   клиент подключения TINKOFF INVEST API.
        account (tinkoff.invest.Account): счет TINKOFF INVEST API.
        df_dict_instr (dict): словарь инструментов

        Returns:
            pd.DataFrame: список открытых позиций
    '''

    df_account = pd.DataFrame(columns=['id', 'Наименование', 'Количество', 'Тип актива'])
    # Получение позиций
    positions = client.operations.get_positions(account_id=account.id)
    # Заполнение DataFrame исходного счета 
    # Акции, фонды
    for pos in positions.securities:
        df_account = pd.concat([df_account,
                                pd.DataFrame({'id': [pos.figi],
                                              'Наименование': [df_dict_instr['name'].get(pos.figi, pos.figi)],
                                              'Количество': [pos.balance],
                                              'Тип актива': ['Акции, фонды']})],
                                             ignore_index=True)
    # Фьючерсы
    for pos in positions.futures:
        df_account = pd.concat([df_account,
                                pd.DataFrame({'id': [pos.figi],
                                              'Наименование': [df_dict_instr['name'].get(pos.figi, pos.figi)],
                                              'Количество': [pos.balance],
                                              'Тип актива': ['Фьючерсы']})],
                                             ignore_index=True)
    # Валюта
    for pos in positions.money:
        df_account = pd.concat([df_account,
                                pd.DataFrame({'id': [pos.currency],
                                              'Наименование': [df_dict_instr['name'].get(pos.currency, pos.currency)],
                                              'Количество': [pos.units + pos.nano/1e9],
                                              'Тип актива': ['Валюта']})],
                                             ignore_index=True)
    return df_account


def get_account_difference(df_account_source: pd.DataFrame, 
                           df_account_target: pd.DataFrame, 
                           ratio_account: float, 
                           df_dict_instr: pd.DataFrame) -> tuple:
    ''' Функция получения заданий на покупку/продажу, данные возвращаются 
        в формате pandas.DataFrame.
       
       Args:
        df_account_source (pd.DataFrame): состав исходного счета
        df_account_target (pd.DataFrame): состав целевого счета
        ratio_account (float): коэффициент сделок, устанавливается в зависимости
                               от соотношения размера целевого счета относительно
                               исходного                           
        df_dict_instr (pd.DataFrame): словарь инструментов

        Returns:
            pd.DataFrame: задание на покупку
            pd.DataFrame: задание на продажу
    '''
    
    df_for_buy = pd.DataFrame(columns=['id', 'Количество лотов', 'Тип актива'])
    df_for_sell = pd.DataFrame(columns=['id', 'Количество лотов', 'Тип актива'])
    # Удаляем из аккаунтов rub
    df_account_source = df_account_source.drop(df_account_source[df_account_source['id'] == 'rub'].index.tolist(), axis=0)
    df_account_target = df_account_target.drop(df_account_target[df_account_target['id'] == 'rub'].index.tolist(), axis=0)
    # Переводим количество активов в количество лотов
    df_account_source['Количество'] = df_account_source['Количество']//df_account_source['id'].map(df_dict_instr['lot'])
    df_account_target['Количество'] = df_account_target['Количество']//df_account_target['id'].map(df_dict_instr['lot'])
    # Применяем коэффициент на количество лотов исходного счета 
    df_account_source['Количество'] = df_account_source['Количество'] * ratio_account
    df_account_source['Количество'] = df_account_source['Количество'].apply(lambda x: int(math.floor(x)))
    
    # Вычисление заданий на покупку/продажу
    for i in df_account_source.index.to_list():
        # Активы, которые есть на исходном счете и нет на целевом попадают в buy
        id_source = df_account_source['id'].loc[i]
        if df_account_target[df_account_target['id'] == id_source].shape[0] == 0:
            df_for_buy = pd.concat([df_for_buy,
                                    pd.DataFrame({'id': [df_account_source['id'].loc[i]],
                                                  'Количество лотов': [df_account_source['Количество'].loc[i]],
                                                  'Тип актива': [df_account_source['Тип актива'].loc[i]]})],
                                                 ignore_index=True)
        # Вычисление частичных открытий/закрытий
        else:
            count_lot_source = df_account_source['Количество'].loc[i]
            count_lot_target = df_account_target[df_account_target['id'] == id_source]['Количество'].iloc[0]
            if count_lot_source > count_lot_target:
                df_for_buy = pd.concat([df_for_buy,
                                        pd.DataFrame({'id': [df_account_source['id'].loc[i]],
                                                      'Количество лотов': [count_lot_source - count_lot_target],
                                                      'Тип актива': [df_account_source['Тип актива'].loc[i]]})],
                                                    ignore_index=True)
            elif count_lot_source < count_lot_target:
                df_for_sell = pd.concat([df_for_sell,
                                         pd.DataFrame({'id': [df_account_source['id'].loc[i]],
                                                       'Количество лотов': [count_lot_target - count_lot_source],
                                                       'Тип актива': [df_account_source['Тип актива'].loc[i]]})],
                                                      ignore_index=True)
                
    # Активы, которых нет на исходном счете и есть на целевом попадают в sell
    for i in df_account_target.index.to_list():
        # Активы, которые есть на исходном счете и нет на целевом попадают в buy
        id_target = df_account_target['id'].loc[i]
        if df_account_source[df_account_source['id'] == id_target].shape[0] == 0:
            df_for_sell = pd.concat([df_for_sell,
                                     pd.DataFrame({'id': [df_account_target['id'].loc[i]],
                                                   'Количество лотов': [df_account_target['Количество'].loc[i]],
                                                   'Тип актива': [df_account_target['Тип актива'].loc[i]]})],
                                                  ignore_index=True)
    # Очистка от нулевых значений (требуется, если коэффициент ratio_account < 1)
    df_for_sell = df_for_sell[df_for_sell['Количество лотов'] > 0]
    df_for_buy = df_for_buy[df_for_buy['Количество лотов'] > 0]
    return (df_for_buy, df_for_sell)
    
    
def start_deal_tasks(client: Client, 
                     account: Account,
                     df_for_deal: pd.DataFrame,
                     buy_sell: bool) -> pd.DataFrame:
    ''' Функция исполнения заданий на покупку/продажу, возвращается
        список неисполненных заданий в формате pandas.DataFrame.
       
       Args:
        client (tinkoff.invest.Client):   клиент подключения TINKOFF INVEST API.
        account (tinkoff.invest.Account): счет TINKOFF INVEST API.
        df_for_deal (pd.DataFrame): задания на покупку
        buy_sell (bool): задания на продажу

        Returns:
            pd.DataFrame: неисполненные задания
    '''
    
    df_not_deal = pd.DataFrame(columns=['id', 'Количество лотов', 'Сообщение'])
    # Продажа
    for i in df_for_deal.index.to_list():
        # Получение статуса торговли инструментом, 
        # доступности исполнения заявки по рынку
        resp_status = client.market_data.get_trading_status(figi=df_for_deal['id'].loc[i])
        trading_status = resp_status.trading_status
        may_market = resp_status.market_order_available_flag
        if may_market and trading_status == SecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING:
            #===================================================================================================
            try:
                # Исполнение ордера
                order_response = client.orders.post_order(quantity=df_for_deal['Количество лотов'].loc[i],
                                                          direction=OrderDirection.ORDER_DIRECTION_BUY if buy_sell else OrderDirection.ORDER_DIRECTION_SELL,
                                                          account_id=account.id,
                                                          order_type=OrderType.ORDER_TYPE_MARKET,
                                                          instrument_id=df_for_deal['id'].loc[i])
                # Получение статуса, сообщения
                report_status, report_message = order_response.execution_report_status, order_response.message
                # Если ордер не был исполнен, добавляем в неисполненные задания
                if report_status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL:
                    df_not_deal = pd.concat([df_not_deal,
                                             pd.DataFrame({'id': [df_for_deal['id'].loc[i]],
                                                           'Количество лотов': [df_for_deal['Количество лотов'].loc[i]],
                                                           'Сообщение': [f'Статус: {report_status}, сообщение: {report_message}']})],
                                                          ignore_index=True)
            except Exception as e:
                # Обработка исключений с более детальной информацией.
                raise RuntimeError(f"Ошибка при размещении ордера для '{df_for_deal['id'].loc[i]}'. {e}.")
            #===================================================================================================
        elif not may_market:
            df_not_deal = pd.concat([df_not_deal,
                                     pd.DataFrame({'id': [df_for_deal['id'].loc[i]],
                                                   'Количество лотов': [df_for_deal['Количество лотов'].loc[i]],
                                                   'Сообщение': ['Недоступно выставления рыночной заявки']})],
                                                  ignore_index=True)
        else:
            df_not_deal = pd.concat([df_not_deal,
                                     pd.DataFrame({'id': [df_for_deal['id'].loc[i]],
                                                   'Количество лотов': [df_for_deal['Количество лотов'].loc[i]],
                                                   'Сообщение': ['Статус торгов отличен от нормального']})],
                                                  ignore_index=True)
    return df_not_deal
    

if __name__ == "__main__":
    while True:
        try:
            try:
                main()
            except KeyboardInterrupt:
                print('Работа скрипта остановлена...')
                break
        except Exception:
            continue