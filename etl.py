import pandas as pd
import requests
import sqlite3
import time
from tqdm import tqdm  

DB_NAME = "movies.db"
OMDB_API_KEY = "fdf01f03" 
OMDB_URL = "http://www.omdbapi.com/"

def extract_data():
    """Read movies.csv and ratings.csv into DataFrames"""
    try:
        movies = pd.read_csv("movies.csv")
        ratings = pd.read_csv("ratings.csv")
        print(f"Extracted {len(movies)} movies and {len(ratings)} ratings.")
        return movies, ratings
    except FileNotFoundError:
        print("Error: movies.csv or ratings.csv not found in this folder.")
        raise


def fetch_omdb_data(title):
    """Fetch movie details from the OMDb API by title."""
    try:
        clean_title = title

        if "(" in clean_title:
            clean_title = clean_title.split("(")[0].strip()

        if "," in clean_title:
            parts = [p.strip() for p in clean_title.split(",")]
            if len(parts) == 2 and parts[1].lower() in ["the", "a", "an"]:
                clean_title = f"{parts[1]} {parts[0]}".strip()

        params = {"t": clean_title, "apikey": OMDB_API_KEY}
        response = requests.get(OMDB_URL, params=params, timeout=5)
        data = response.json()

        if data.get("Response") == "True":
            print(f"Found: {clean_title}")
            return {
                "director": data.get("Director"),
                "plot": data.get("Plot"),
                "box_office": data.get("BoxOffice"),
                "year": int(data.get("Year", "0").split("–")[0]) if data.get("Year") else None
            }
        else:
            print(f"Not found: {clean_title}")
            return {"director": None, "plot": None, "box_office": None, "year": None}

    except Exception as e:
        print(f" Error fetching '{title}': {e}")
        return {"director": None, "plot": None, "box_office": None, "year": None} 
def transform_data(movies):
    """Clean, enrich, and transform movie data."""
    enriched_data = []

    for _, row in tqdm(movies.head(20).iterrows(), total=20, desc="🎬 Enriching movies"):
        info = fetch_omdb_data(row['title'])
        enriched_data.append({
            "movie_id": row['movieId'],
            "title": row['title'],
            "genres": row['genres'],
            "director": info["director"],
            "plot": info["plot"],
            "box_office": info["box_office"],
            "year": info["year"]
        })
        time.sleep(0.2)  
    print(f" Enriched {len(enriched_data)} movies with OMDb details.")
    return pd.DataFrame(enriched_data)

def load_data(movies_df, ratings_df):
    """Load final data into SQLite database."""
    conn = sqlite3.connect(DB_NAME)
    movies_df.to_sql("movies", conn, if_exists="replace", index=False)
    ratings_df.to_sql("ratings", conn, if_exists="replace", index=False)
    conn.close()
    print(f" Data successfully loaded into {DB_NAME}")

def main():
    print(" Starting ETL process...")
    movies, ratings = extract_data()
    print(" Transforming and enriching data with OMDb API...")
    enriched_movies = transform_data(movies)
    print(" Loading data into database...")
    load_data(enriched_movies, ratings)
    print(" ETL completed successfully! Database created as movies.db")

if __name__ == "__main__":
    main()
