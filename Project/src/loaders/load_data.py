from src.loggers.logger import *


def build_report(connect):
    print_log('Создание отчёта...')

    sq = 'SELECT "build_report"()'  # Вызов функции создания отчёта
    cursor = connect.cursor()

    try:
        cursor.execute(sq)
    except Exception as error:
        print_log('При выполнении запроса возникла ошибка:', True)
        print_log(error, True)
        print_log('Запрос: ' + str(sq), True)

    print_log('Создание отчёта завершено')


def distribution_of_records(connect):
    print_log('Распределение записей из временной таблицы stg_transactions...')

    sq = 'SELECT "distribution_of_records"()'  # Вызов функции распределения записей
    cursor = connect.cursor()

    try:
        cursor.execute(sq)
    except Exception as error:
        print_log('При выполнении запроса возникла ошибка:', True)
        print_log(error, True)
        print_log('Запрос: ' + str(sq), True)

    print_log('Распределение записей завершено')


def insert_data(transaction_df, connect):
    print_log('Загрузка транзакций в временную таблицу stg_transactions...')

    values = ', '.join(list('?' * len(transaction_df.columns)))  # Генерация маркеров параметров
    sq = f'''INSERT INTO stg_transactions
                VALUES({values});'''
    cursor = connect.cursor()

    try:
        cursor.executemany(sq, transaction_df.values.tolist())
    except Exception as error:
        print_log('При выполнении запроса возникла ошибка:', True)
        print_log(error, True)
        print_log('Запрос: ' + str(sq), True)
        exit(-1)

    print_log('Загрузка транзакций завершена')

    distribution_of_records(connect)
    build_report(connect)