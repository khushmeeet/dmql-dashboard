import psycopg2
import os


def get_db_connection():
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    return conn