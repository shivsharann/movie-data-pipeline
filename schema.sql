CREATE TABLE IF NOT EXISTS movies (
    movie_id INTEGER PRIMARY KEY,
    title TEXT,
    genres TEXT,
    director TEXT,
    plot TEXT,
    box_office TEXT,
    year INTEGER
);

CREATE TABLE IF NOT EXISTS ratings (
    rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    movie_id INTEGER,
    rating REAL,
    timestamp TEXT,
    FOREIGN KEY(movie_id) REFERENCES movies(movie_id)
);
