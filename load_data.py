import pandas as pd
import mysql.connector
from mysql.connector import Error

# 1. Читаем CSV с фильмами (скачай любой с Kaggle или создай сам)
# Пример данных movies.csv:
# id,title,year,genre,rating
# 1,The Matrix,1999,Sci-Fi,8.7
# 2,Inception,2010,Sci-Fi,8.8
# 3,The Godfather,1972,Crime,9.2

df = pd.read_csv('movies.csv')

# 2. Подключаемся к MySQL
try:
    connection = mysql.connector.connect(
        host='localhost',
        user='твой_логин',
        password='твой_пароль',
        database='movies_db'  # создай базу заранее через CREATE DATABASE movies_db;
    )
    
    cursor = connection.cursor()
    
    # 3. Создаём таблицу 
    cursor.execute("DROP TABLE IF EXISTS movies")
    
    create_table_query = """
    CREATE TABLE movies (
        id INT PRIMARY KEY,
        title VARCHAR(255),
        year INT,
        genre VARCHAR(100),
        rating FLOAT
    )
    """
    cursor.execute(create_table_query)
    
    # 4. Загружаем данные из pandas в MySQL
    for row in df.itertuples():
        insert_query = """
        INSERT INTO movies (id, title, year, genre, rating)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (row.id, row.title, row.year, row.genre, row.rating))
    
    connection.commit()
    print(f"Загружено {len(df)} записей")
    
except Error as e:
    print(f"Ошибка: {e}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()