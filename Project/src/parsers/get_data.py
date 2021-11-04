import os
from pandas import read_excel
from src.configs.config import *
from src.loggers.logger import *


def add_dir(file_name):
    return EXCELS_DIR + file_name


def search_xlsx(files):
    return [f for f in files if f.lower().endswith('.xlsx')]


def get_last_file(last_filename):
    try:
        # Путь последнего файла
        file_path = max(search_xlsx(map(add_dir, os.listdir(EXCELS_DIR))), key=os.path.getctime)
    except:
        return None

    if file_path == last_filename:
        return None

    file = open(file_path, 'rb')
    print_log(f'Актуальный excel file {file.name} загружен')
    return file


def max_trans_id(connect):
    print_log('Получение номера последней транзакции...')
    sq = """SELECT MAX(trans_id) 
            FROM fact_transactions;"""
    cursor = connect.cursor()

    try:
        cursor.execute(sq)
    except Exception as error:
        print_log('При выполнении запроса возникла ошибка:', True)
        print_log(error, True)
        print_log('Запрос: ' + str(sq), True)
        exit(-1)

    trans_id = cursor.fetchall()[0]

    if None in trans_id:
        trans_id = -1
        print_log('Ни одна транзакция ещё не загружена')
    else:
        trans_id = trans_id[0]
        print_log('Номер последней транзакции: ', trans_id)
    return trans_id


def parsing_excel(file, last_trans_id):
    print_log('Чтение excel...')
    transactions = None

    try:
        transactions = read_excel(file, converters={
            'card': str, 'account': str, 'phone': str, 'client': str, 'passport': str
        })
    except Exception as error:
        print_log('При чтении файла возникла ошибка:', True)
        print_log(error, True)
        exit(-1)

    # Отбрасываем записи, которые уже в БД
    transactions = transactions.loc[transactions['trans_id'] > last_trans_id]

    numbers = transactions['phone']
    transactions['phone'] = list(map(parsing_number, numbers))  # Приводим телефонные номера к общему виду

    print_log('Чтение excel завершено')
    return transactions


def parsing_number(phone_number):
    digits = list(map(str, list(range(10))))
    cur_num = ''

    for i in phone_number:
        if i in digits:
            cur_num += i

    cur_num = '+7' + cur_num[1:]
    return cur_num
