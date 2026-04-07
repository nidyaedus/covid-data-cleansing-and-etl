# Project 1: COVID-19 ETL Pipeline

This project is my first end-to-end ETL (Extract, Transform, Load) pipeline, built as I step into the world of data engineering. My goal wasn't just to write working code; it was to understand the journey of data from its source, through cleansing, to being written to a cloud database and to learn the best practices along the way.

## Technologies & Tools Used

* **Python & Pandas:** Used for extracting data from the web and performing complex data transformations and cleansing.
* **SQLAlchemy:** Used as the database toolkit to establish a secure and reliable connection bridge between the Python environment and the database.
* **Neon.tech (Serverless PostgreSQL):** Used as the target cloud database. **Why Neon?** I chose Neon because it provides a fully managed, serverless PostgreSQL environment. This eliminated the overhead of installing, configuring, and maintaining a local database server, allowing me to focus entirely on building the data pipeline and experiencing cloud integration.

## What Did I Learn in This Project?

While building this pipeline, I practically experienced the following concepts:

### 1. Extract
* Instead of downloading data manually, I learned how to use `pandas` to pull data directly from a raw CSV URL on GitHub into the Python environment.

### 2. Transform (Data Cleansing and Transformation)
* **Wide to Long Format:** I used the `melt()` function to convert "wide" format data (where every new day was a new column) into a "long" format, which is much more suitable for database architecture.
* **Data Type Management:** I converted date columns from string format to the standard `datetime` format.
* **Filtering:** I used the `~` (NOT) operator and `.isin()` logic to filter out unwanted or invalid records (e.g., cruise ships) from the dataset.

### 3. Load (Writing to a Cloud Database)
* **Security:** Instead of hardcoding my database password, I used the `getpass` library to securely enter it at runtime.
* **Handling Duplicates:** I grasped the logic behind the `if_exists='replace'` parameter in pandas' `to_sql()` function to prevent duplicate records when loading data.

### 4. Validation
* After sending the data to the database, without leaving the Python environment, I executed an SQL query against the database using `pd.read_sql()` to confirm that the data was loaded in the correct format.

---

## File Structure

* **`main.ipynb`**: My interactive notebook where I examined the data step-by-step and visually tracked the tables and outputs during the development phase.
* **`requirements.txt`**: The list of Python libraries required to run the project.
