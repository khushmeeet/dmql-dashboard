import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
import sys


def insert_query(conn, sql, df):
    rows = [tuple(i) for i in df.to_numpy()]
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_values(cursor, sql, rows)
        conn.commit()
    except Exception as e:
        print('Error.......')
        print(e)
        cursor.close()
        conn.rollback()


def insert_state_relation(data):
    state = pd.DataFrame({'id': [i for i in range(1,53)], 'region_name':data['region_name'].unique()})
    insert_query(conn, 'insert into state (id, state_name) values %s', state)
    print('Data insertion into State relation is complete')
    return state


def insert_city_relation(data, state):
    citydf = data[['municipality', 'region_name']]
    citydf = citydf.dropna()
    citydf = citydf.drop_duplicates()
    citydf = citydf.reset_index(drop=True)
    citydf['state_id'] = citydf['region_name'].apply(lambda s:state.index[state['region_name']==s].to_list()[0]+1)
    citydf = citydf.drop(['region_name'], axis=1)
    citydf.insert(0, 'id', [i for i in range(1,366)])
    insert_query(conn, 'insert into city (id, city_name, state_id) values %s', citydf)
    print('Data insertion into City relation is complete')
    return citydf


def insert_aiport_relation(data, state, citydf):
    airport = data[['name', 'airport', 'type', 'latitude_deg', 'longitude_deg', 'region_name', 'municipality']]
    airport = airport.dropna()
    airport = airport.drop_duplicates()
    airport = airport.reset_index(drop=True)
    airport['type'] = airport['type'].replace(['medium_airport', 'large_airport', 'small_airport'],['medium','large', 'small'])
    airport['city_id'] = airport['municipality'].apply(lambda s: citydf.index[citydf['municipality']==s].to_list()[0]+1)
    airport['state_id'] = airport['region_name'].apply(lambda s: state.index[state['region_name']==s].to_list()[0]+1)
    airport = airport.drop(['municipality', 'region_name'], axis=1)
    airport.insert(0, 'id', [i for i in range(1,len(airport)+1)])
    airport_details = airport.drop(['name', 'latitude_deg', 'longitude_deg'], axis=1)
    insert_query(conn, 'insert into airport (id, iata, type, city_id, state_id) values %s', airport_details)
    print('Data insertion into Airport relation is complete')
    return airport


def insert_airport_loc_relation(data):
    airport_loc = airport[['id', 'name', 'latitude_deg', 'longitude_deg']]
    insert_query(conn, 'insert into airport_loc (id, airport_name, latitude, longitude) values %s', airport_loc)
    print('Data insertion into Airport_loc relation is complete')

def insert_airline_relation(data):
    airline = data[['carrier_name', 'IATA', 'ICAO', 'Callsign']]
    airline = airline.drop_duplicates()
    airline = airline.drop(airline.index[[17]])
    airline = airline.reset_index(drop=True)
    airline.insert(0, 'id', [i for i in range(1,len(airline)+1)])
    insert_query(conn, 'insert into airline (id, airline_name, iata, icao, callsign) values %s', airline)
    print('Data insertion into Airline relation is complete')
    return airline


def insert_flight_relation(data, airport, airline):
    flight = data.iloc[:,[1,2,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]]
    flight = flight.dropna(axis=0, how='any', thresh=14)
    flight = flight.drop(flight.index[flight['airport']=='ECP'])
    flight['airline_id'] = flight['carrier_name'].apply(lambda s: airline.index[airline['carrier_name']==s].to_list()[0]+1)
    flight['airport_id'] = flight['airport'].apply(lambda s: airport.index[airport['airport']==s].to_list()[0]+1)
    flight = flight.drop(['carrier_name', 'airport'], axis=1)
    flight.insert(0, 'id', [i for i in range(1,len(flight)+1)])
    insert_query(conn, """insert into flight (id,
                        year,
                        month,
                        arr_flights,
                        arr_del15,
                        carrier_ct,
                        weather_ct,
                        nas_ct,
                        security_ct,
                        late_aircraft_ct,
                        arr_cancelled,
                        arr_diverted,
                        arr_delay,
                        carrier_delay,
                        weather_delay,
                        nas_delay,
                        security_delay,
                        late_aircraft_delay,
                        airline_id,
                        airport_id)  values %s""", flight)
    print('Data insertion into Flight relation is complete')


if __name__ == '__main__':
    data = pd.read_csv('final-data.csv')
    conn = psycopg2.connect(f'dbname={sys.argv[1]} user={sys.argv[2]} password={sys.argv[3]}', host="127.0.0.1", port="5432")
    state = insert_state_relation(data)
    city = insert_city_relation(data, state)
    airport = insert_aiport_relation(data, state, city)
    insert_airport_loc_relation(airport)
    airline = insert_airline_relation(data)
    insert_flight_relation(data, airport, airline)