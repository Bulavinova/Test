# Тест и классы для работы с базой данных

from datetime import datetime
import mysql.connector
from local_settings import dbconfig
from queries_templates import *

class MixinMySQLQuery:
    def simple_select(self, query, values):
        try:
            print(f"Executing query: {query} with values: {values}")
            self.cursor.execute(query, values)
            rows = self.cursor.fetchall()
            return rows if rows is not None else []
        except Exception as e:
            print(f"*{e.__class__.__name__}: {e}")
            return []

    def is_exist_table(self, get_table_name_query, table_name) -> bool:
        try:
            query = get_table_name_query.format(table_name)
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            if not result:
                print(f"*The table <{table_name}> does not exist!")
                return False
            return True
        except Exception as e:
            print(f"*{e.__class__.__name__}: {e}")

class MixinMySQLConnection(MixinMySQLQuery):
    def __init__(self, dbconfig):
        self.dbconfig = dbconfig
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connection = mysql.connector.connect(**self.dbconfig)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

def some_function_that_queries_db(db):
    try:
        query = search_by_keyword_query  # Используем существующий запрос
        params = ('%film%', '%film%')  # Параметры запроса
        print(f"Executing query: {query} with params: {params}")
        rows = db.simple_select(query, params)  # Выполнение запроса

        # Вывод количества строк и самих данных для отладки
        print(f"Number of rows fetched: {len(rows)}")
        print(f"Rows fetched: {rows}")

        if not rows:
            print("Warning: No rows returned from the query.")
        else:
            print(f"First row: {rows[0]}")

        assert len(rows) > 0, "Error: No rows returned from the query!"

    except Exception as e:
        print(f"Exception occurred: {e}")

if __name__ == '__main__':
    with MixinMySQLConnection(dbconfig) as db:
        # 1. Проверка создания подключения
        db.cursor.execute('SELECT * FROM category LIMIT 1;')
        rows = db.cursor.fetchall()
        print(f"Rows fetched for connection check: {rows}")
        e1 = (1, 'Action', datetime(2006, 2, 15, 4, 46, 27))  # Обновить если нужно
        assert rows[0] == e1, "Error!"

        # 2. Проверка метода simple_select()
        rows = db.simple_select(search_by_keyword_query, ('%category%', '%category%'))
        print(f"Rows fetched for simple_select: {rows}")
        assert len(rows) > 0, "Error!"  # Проверка, что количество полученных строк больше нуля

        # 3. Проверка метода .is_exist_table()
        # 3.1 если есть имя таблицы
        is_exist = db.is_exist_table("SHOW TABLES LIKE '{}'", 'category')
        assert is_exist == True, "Error!"

        # 3.2 если нет имя таблицы
        is_exist = db.is_exist_table("SHOW TABLES LIKE '{}'", 'users1')
        assert is_exist == False, "Error!"

        # 4. Проверка функции some_function_that_queries_db
        some_function_that_queries_db(db)

    print("Тестирование успешно!")

