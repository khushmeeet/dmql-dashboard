from flask import render_template, Blueprint, request
import psycopg2
from ..utils import get_db_connection


home_mod = Blueprint('home', __name__, template_folder='templates', static_folder='static')


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