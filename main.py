from fastapi import FastAPI, HTTPException
import mysql.connector
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='твой_логин',
        password='твой_пароль',
        database='movies_db'
    )

class MovieCreate(BaseModel):
    title: str
    year: int
    genre: str
    rating: float

# 1. GET /movies - поиск по названию и жанру с пагинацией
@app.get("/movies")
def get_movies(title: Optional[str] = None, 
               genre: Optional[str] = None,
               page: int = 1, 
               limit: int = 10):
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = "SELECT * FROM movies WHERE 1=1"
    params = []
    
    if title:
        query += " AND title LIKE %s"
        params.append(f"%{title}%")
    if genre:
        query += " AND genre = %s"
        params.append(genre)

    offset = (page - 1) * limit
    query += " LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    cursor.execute(query, params)
    movies = cursor.fetchall()
    
    cursor.close()
    conn.close()
    return {"movies": movies, "page": page, "limit": limit}

# 2. GET /movies/{id} - получить фильм по ID
@app.get("/movies/{movie_id}")
def get_movie(movie_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT * FROM movies WHERE id = %s", (movie_id,))
    movie = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if not movie:
        raise HTTPException(status_code=404, detail="Фильм не найден")
    return movie

# 3. POST /movies - создать новый фильм
@app.post("/movies")
def create_movie(movie: MovieCreate):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT MAX(id) as max_id FROM movies")
    max_id = cursor.fetchone()[0] or 0
    new_id = max_id + 1
    
    cursor.execute(
        "INSERT INTO movies (id, title, year, genre, rating) VALUES (%s, %s, %s, %s, %s)",
        (new_id, movie.title, movie.year, movie.genre, movie.rating)
    )
    conn.commit()
    
    cursor.close()
    conn.close()
    return {"id": new_id, **movie.dict()}

# 4. PUT /movies/{id} - обновить фильм
@app.put("/movies/{movie_id}")
def update_movie(movie_id: int, movie: MovieCreate):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute(
        "UPDATE movies SET title = %s, year = %s, genre = %s, rating = %s WHERE id = %s",
        (movie.title, movie.year, movie.genre, movie.rating, movie_id)
    )
    conn.commit()
    
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Фильм не найден")
    
    cursor.close()
    conn.close()
    return {"id": movie_id, **movie.dict()}

# 5. DELETE /movies/{id} - удалить фильм
@app.delete("/movies/{movie_id}")
def delete_movie(movie_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM movies WHERE id = %s", (movie_id,))
    conn.commit()
    
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Фильм не найден")
    
    cursor.close()
    conn.close()
    return {"message": "Фильм удален"}