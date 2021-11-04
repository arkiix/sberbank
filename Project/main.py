import time
import pyodbc
from src import *

def connect_db():
    # Строка подключения к БД
    cs = f'DRIVER={DRIVER_NAME};User={UID};Password={PWD};Database={DATABASE};Server={HOSTNAME};Port={PORT};'
    try:
        connect = pyodbc.connect(cs)
        print_log('База данных успешно подключена')
        return connect
    except Exception as error:
        print_log('При подключении к БД возникла ошибка:', True)
        print_log(error, True)
        exit(-1)


def run():
    last_filename = ''
    while True:
        file = get_last_file(last_filename)  # Импортирование актуального эксель файла

        if file is not None:
            connect = connect_db()  # Подключение к БД
            last_trans_id = max_trans_id(connect)  # Получение последнего номера транзакции
            transactions_df = parsing_excel(file, last_trans_id)  # Парсинг эксель файла
            if len(transactions_df) == 0:
                print_log('Новые транзакции не найдены')
            else:
                insert_data(transactions_df, connect)  # Вставка данных в БД
            connect.close()
            print_log('Подключение к БД закрыто')
            last_filename = file.name
        time.sleep(15)


if __name__ == '__main__':
    run()