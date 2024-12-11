import os
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine
import psycopg2

load_dotenv(find_dotenv())

ANALYSIS_URL = os.getenv("analysis_url")
ENGINE = create_engine(ANALYSIS_URL)
DATA_ROOT = os.getenv("data_root")

# establishing the connection
conn = psycopg2.connect(
    database="transit_directness",
    user='postgres',
    password='root',
    host='localhost',
    port='5432'
)