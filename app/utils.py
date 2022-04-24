import psycopg2


def get_db_connection():
    conn = psycopg2.connect("dbname=test_flight user=postgres password=434649", host="127.0.0.1", port="5432")
    return conn