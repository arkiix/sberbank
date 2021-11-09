# sberbank

Индивидуальный проект сбербанк.

## Usage

### Start daemon (for linux)

    python3 start_daemon.py


### Start main

    python3 main.py


## Installation

### DB (GreenPlum)
    Перед запуском ETL процесса, исполните все sql скрипты из директории Project/sql/

### Python
  Для работы программы необходимо установить [ODBC driver](https://www.cdata.com/drivers/greenplum/download/odbc/)

  #### Requirements
* python-daemon
* pyodbc
* pandas
    
  #### Linux specific Instructions
      git clone https://github.com/arkiix/sberbank/
      cd sberbank/Project/Python
      pip3 install -r requirements.txt

  #### MacOS-specific Instructions
  If you fail to install it with the help of this command:

      pip3 install -r requirements.txt
  Try the following one:

      easy_install `cat requirements.txt`
