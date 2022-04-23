from flask import render_template, Blueprint, request
import psycopg2

home_mod = Blueprint('home', __name__, template_folder='templates', static_folder='static')


def get_db_connection():
    conn = psycopg2.connect("dbname=test_flight user=postgres password=434649", host="127.0.0.1", port="5432")
    return conn


@home_mod.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        query = request.form['query']
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('home/index.html', results=results, cur=cur)
    else:
        return render_template('home/index.html')