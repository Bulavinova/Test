# SQL-запросы
from mysql.connector.aio import cursor

# 1 Поиск по ключевому слову
search_by_keyword_query = """
SELECT title, description
FROM film
WHERE title LIKE %s OR description LIKE %s 
LIMIT 10;
"""

# 2 Поиск по году
search_by_year_query = """
SELECT title, release_year, rental_rate
FROM film
WHERE release_year = %s
ORDER BY rental_rate DESC
LIMIT 10;
"""

# 3 Топ по жанру
search_by_genre_query = """
SELECT f.title, c.name AS genre, f.rental_rate, f.release_year
FROM film f
LEFT JOIN film_category fc ON f.film_id = fc.film_id
LEFT JOIN category c ON fc.category_id = c.category_id
WHERE c.name = %s
ORDER BY f.rental_rate DESC, f.release_year DESC
LIMIT 10;
"""

# 4 Топ по году
search_movies_by_year_query = """
SELECT f.title, f.release_year, f.rental_rate
FROM film f
WHERE f.release_year = %s
ORDER BY f.rental_rate DESC, f.release_year DESC;
"""

# 5 Поиск по жанру и году
search_by_genre_and_year_query = """
SELECT f.title, c.name AS genre, f.release_year
FROM film f
LEFT JOIN film_category fc ON f.film_id = fc.film_id
LEFT JOIN category c ON fc.category_id = c.category_id
WHERE c.name = %s AND f.release_year IN ({})
ORDER BY f.release_year;
"""

# 6 Получение списка жанров, категорий
get_categories_query = """
SELECT name FROM category;
"""

# 7 Создание таблицы для хранения поисковых запросов по ключевым словам
create_search_queries_table = """
CREATE TABLE IF NOT EXISTS search_queries (
    id INT AUTO_INCREMENT PRIMARY KEY,
    keyword VARCHAR(255) UNIQUE NOT NULL,
    search_count INT DEFAULT 1,
    last_search DATETIME DEFAULT NOW()
);
"""

# 8 Сохранение поискового запроса
save_search_keyword_query = """
INSERT INTO search_queries (keyword, search_count, last_search)
VALUES (%s, 1, NOW())
ON DUPLICATE KEY UPDATE search_count = search_count + 1, last_search = NOW();
"""

# 9 Создание таблицы для хранения запросов по жанру и году
create_search_genre_year_table = """
CREATE TABLE IF NOT EXISTS search_genre_year (
    id INT AUTO_INCREMENT PRIMARY KEY,
    genre VARCHAR(255),
    year INT DEFAULT NULL,
    search_count INT DEFAULT 1,
    last_search TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    query VARCHAR(255) DEFAULT NULL
);
"""

# 10 Сохранение запроса по жанру и году
save_genre_year_query = """
INSERT INTO search_genre_year (genre, year, search_count, last_search, query)
VALUES (%s, %s, 1, CURRENT_TIMESTAMP, %s)
ON DUPLICATE KEY UPDATE search_count = search_count + 1, last_search = CURRENT_TIMESTAMP;
"""

# 11 Получение списка популярных запросов по жанру и году
get_popular_genre_year_queries_query = """
SELECT genre, year, search_count
FROM search_genre_year
ORDER BY search_count DESC;
"""

# 12 Запрос для получения популярных ключевых слов
get_popular_queries_query1 = """
SELECT search_queries, search_count
FROM search_queries
ORDER BY search_count DESC
LIMIT 10;
"""

# 13 Запрос для получения популярных ключевых слов
get_popular_queries_query = """
SELECT keyword, search_count
FROM search_queries
ORDER BY search_count DESC
LIMIT %s;
"""

