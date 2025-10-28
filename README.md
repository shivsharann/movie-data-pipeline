
Movie Data ETL Pipeline

This project extracts, transforms, and loads movie data from CSV files into a SQLite database, 
enriches it using the OMDb API, and performs SQL analysis.

Project Structure
- `etl_pipeline.py` — Python ETL pipeline
- `movies.db` — Final SQLite database
- `queries.sql` — SQL analytical queries
- `movies.csv`, `ratings.csv` — Input datasets

How to Run
1. Run `python etl_pipeline.py` to execute the ETL pipeline.
2. Open `movies.db` using VS Code SQLite viewer.
3. Run queries from `queries.sql` for analysis.


