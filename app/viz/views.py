from flask import render_template, Blueprint, request
from ..utils import get_db_connection
import pandas as pd
import plotly
import plotly.express as px
import json


viz_mod = Blueprint('viz', __name__, template_folder='templates', static_folder='static')


def execute_query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    return results, cur


@viz_mod.route('/viz', methods=['GET', 'POST'])
def viz():
    conn = get_db_connection()
    sql1 = 'select airport_loc.airport_name, count(*) as total_flight from flight, airport_loc where flight.airport_id=airport_loc.id group by airport_loc.airport_name'
    results, cur = execute_query(conn, sql1)
    df = pd.DataFrame(results)
    df.columns = [col[0] for col in cur.description]
    fig1 = px.bar(df, x='airport_name', y='total_flight')
    fig1.update_layout(title="Total number of flights from Jan 2018 to Jan 2022", height=800)
    fig1_html = fig1.to_html(full_html=False)

    sql2 = 'select airport_name, latitude, longitude from airport_loc'
    results, cur = execute_query(conn, sql2)
    df = pd.DataFrame(results)
    df.columns = [col[0] for col in cur.description]
    fig2 = fig = px.scatter_geo(df, lat=df.latitude, lon=df.longitude, hover_name=df.airport_name)
    fig2.update_layout(
        title = 'US Airports Map',
        geo_scope='usa',
    )
    fig2_html = fig2.to_html(full_html=False)

    sql3 = 'select sum(arr_flights) as arrival_flight, sum(arr_del15) as delayed_flight, sum(weather_ct) as weather_delay, sum(nas_ct) as nas_delay, sum(security_ct) as security_delay from flight group by month, year order by year, month'
    results, cur = execute_query(conn, sql3)
    df = pd.DataFrame(results)
    df.columns = [col[0] for col in cur.description]
    fig3 = px.line(df, x=[i for i in range(1,50)], y=df.columns)
    fig3.update_layout(title = 'Flight Statistics per month')
    fig3_html = fig3.to_html(full_html=False)

    sql4 = """with total_flights as (
                select sum(arr_flights+carrier_ct+weather_ct+nas_ct+security_ct+late_aircraft_ct) from flight
            )
            select sum(arr_flights)/(select * from total_flights)*100 as arr_flight_percent,
                sum(carrier_ct)/(select * from total_flights)*100 as weather_ct_percent,
                sum(nas_ct)/(select * from total_flights)*100 as nas_ct_percent,
                sum(security_ct)/(select * from total_flights)*100 as security_ct_percent,
                sum(late_aircraft_ct)/(select * from total_flights)*100 as late_aircraft_ct_percent
            from flight"""
    results, cur = execute_query(conn, sql4)
    df = pd.DataFrame(results)
    df.columns = [col[0] for col in cur.description]
    fig4 = px.pie(values=df.iloc[0,:], names=df.columns)
    fig4.update_layout(title = 'Flight Statistics per month')
    fig4_html = fig4.to_html(full_html=False)
    cur.close()
    conn.close()

    return render_template('viz/index.html', figs=[(sql1, fig1_html), (sql2, fig2_html), (sql3, fig3_html), (sql4, fig4_html)])