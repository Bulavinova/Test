# Основная логика и запуск программы

import logging
from mySQL_manager import MixinMySQLConnection
from local_settings import dbconfig
from queries_templates import *
from prettytable import PrettyTable

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Функции для работы с базой данных

def get_categories(db):
    logger.debug("Fetching categories...")
    categories = db.simple_select(get_categories_query, ())  # Передаем пустой кортеж в качестве values
    return [category[0] for category in categories]

def search_movies_by_year(db, year):
    logger.debug(f"Searching movies by year: {year}")
    result = db.simple_select(search_by_year_query, (year,))
    return result

def search_movies_by_genre(db, genre):
    logger.debug(f"Searching movies by genre: {genre}")
    result = db.simple_select(search_by_genre_query, (genre,))
    return result

def search_movies_by_genre_and_years(db, genre, years):
    query = search_by_genre_and_year_query.format(','.join(['%s'] * len(years)))
    logger.debug(f"Searching movies by genre: {genre} and years: {years}")
    result = db.simple_select(query, (genre, *years))
    return result

def get_search_movies_by_keywords(db, keywords):
    search_keyword = f'%{keywords}%'
    logger.debug(f"Searching movies by keyword: {keywords}")
    result = db.simple_select(search_by_keyword_query, (search_keyword, search_keyword))

    try:
        # Выполнение запроса на сохранение ключевых слов в таблицу search_queries
        db.cursor.execute(save_search_keyword_query, (keywords,))
        # Подтверждение изменений в базе данных
        db.connection.commit()
        logger.debug(f"Keyword '{keywords}' saved successfully.")
    except Exception as e:
        # В случае ошибки откат изменений
        db.connection.rollback()
        logger.error(f"Error while saving search keyword: {e}")

    return result

def get_popular_queries(db):
    logger.debug("Fetching popular queries...")
    limit = 10  # Количество популярных запросов
    result = db.simple_select(get_popular_queries_query, (limit,))
    return result

def main():
    with MixinMySQLConnection(dbconfig) as db:
        # Получение списка доступных жанров перед выводом меню
        genres = get_categories(db)
        print("Доступные жанры:")
        for genre in genres:
            print(f"- {genre}")
        print()  # Пустая строка для разделения списка жанров и основного меню

    while True:
        print(
            """
Введите команду: 
1 - Поиск по ключевому слову 
2 - Поиск по жанру и году 
3 - Популярные запросы 
0 - Выход
"""
        )
        command = input("Ваш выбор: ")

        with MixinMySQLConnection(dbconfig) as db:
            if command == "1":
                keyword = input("Введите ключевое слово: ")
                results = get_search_movies_by_keywords(db, keyword)
                if results:
                    table = PrettyTable(["Title", "Description"])
                    for result in results:
                        table.add_row(result)
                    print(table)
                else:
                    print("Нет фильмов, соответствующих вашему запросу.")

            elif command == "2":
                genre = input("Введите жанр: ")
                years_input = input("Введите годы через запятую: ")
                years = [int(year.strip()) for year in years_input.split(',')]
                results = search_movies_by_genre_and_years(db, genre, years)
                if results:
                    table = PrettyTable(["Title", "Genre", "Release Year"])
                    for result in results:
                        table.add_row(result)
                    print(table)
                else:
                    print(f"Нет фильмов с жанром '{genre}' и годами выпуска {years_input}.")

            elif command == "3":
                popular_queries = get_popular_queries(db)
                if popular_queries:
                    table = PrettyTable(["Keyword", "Search Count"])
                    for query in popular_queries:
                        table.add_row(query)
                    print(table)
                else:
                    print("Нет популярных запросов на данный момент.")

            elif command == "0":
                break
            else:
                print("Неправильная команда. Попробуйте снова.")

if __name__ == "__main__":
    main()
