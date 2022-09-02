import psycopg2
import keyring
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_db():
    try:

        passwd = keyring.get_password("postgres", "postgres")
        connection = psycopg2.connect(user="postgres",
                                      password=passwd,
                                      host="127.0.0.1",
                                      port="5555")
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        sql_create_database = f'create database postgres_db'
        cursor.execute(sql_create_database)
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def create_table():
    try:
        passwd = keyring.get_password("postgres", "postgres")
        connection = psycopg2.connect(user="postgres",
                                      password=passwd,
                                      host="127.0.0.1",
                                      port="5555",
                                      database="postgres_db")
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE trades
                              (IntOperNum    varchar(20) PRIMARY KEY     NOT NULL,
                              AssetID        varchar(20)                 NOT NULL,
                              Price          real                        NOT NULL); '''
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def drop_table():
    try:
        passwd = keyring.get_password("postgres", "postgres")
        connection = psycopg2.connect(user="postgres",
                                      password=passwd,
                                      host="127.0.0.1",
                                      port="5555",
                                      database="postgres_db")
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        drop_table_query = 'DROP TABLE trades'
        cursor.execute(drop_table_query)
        connection.commit()
        print("Таблица успешно удалена в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def insert_test():
    try:
        passwd = keyring.get_password("postgres", "postgres")
        connection = psycopg2.connect(user="postgres",
                                      password=passwd,
                                      host="127.0.0.1",
                                      port="5555",
                                      database="postgres_db")
        cursor = connection.cursor()
        insert_query = """ INSERT INTO trades (IntOperNum, AssetID, Price) VALUES ('mcx123456', '123456', 1000.11)"""
        cursor.execute(insert_query)
        connection.commit()
        print("1 запись успешно вставлена")
        cursor.execute("SELECT * from trades")
        record = cursor.fetchall()
        print("Результат", record)

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


if __name__ == '__main__':
    pass
