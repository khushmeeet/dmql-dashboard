import psycopg2
import psycopg2.extras
import sys


def create_table(sql, conn):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        print('Error.......')
        print(e)
        cursor.close()
        conn.rollback()


if __name__ == '__main__':
    conn = psycopg2.connect(f'dbname={sys.argv[1]} user={sys.argv[2]} password={sys.argv[3]}', host="127.0.0.1", port="5432")

    create_table("""create table state(
                id int primary key,
                state_name varchar(50) not null unique)""", conn)
    print('State relation created successfully')

    
    create_table("""create table city(
                id int primary key,
                city_name varchar(100) not null,
                state_id int,
                constraint in_state foreign key(state_id) references state(id) on delete cascade)""", conn)
    print('City relation created successfully')
    
    create_table("""create table airport(
                id int primary key,
                iata varchar(4),
                type text check (type in ('large', 'medium', 'small', 'closed')),
                city_id int,
                state_id int,
                constraint in_city foreign key(city_id) references city(id) on delete cascade on update cascade,
                constraint in_state foreign key(state_id) references state(id) on delete cascade on update cascade)""", conn)
    print('Airport relation created successfully')

    create_table("""create table airport_loc(
                id int primary key,
                airport_name varchar(200) not null unique,
                latitude numeric,
                longitude numeric)""", conn)
    print('Airport_loc relation created successfully')
    
    create_table("""create table airline(
                id int primary key,
                airline_name varchar(100) not null unique,
                iata varchar(4),
                icao varchar(4),
                callsign varchar(50))""", conn)
    print('Airline relation created successfully')
    
    create_table("""create table flight(
                id int primary key,
                year int not null,
                month int not null,
                arr_flights real,
                arr_del15 real,
                carrier_ct real,
                weather_ct real,
                nas_ct real,
                security_ct real,
                late_aircraft_ct real,
                arr_cancelled real,
                arr_diverted real,
                arr_delay real,
                carrier_delay real,
                weather_delay real,
                nas_delay real,
                security_delay real,
                late_aircraft_delay real,
                airline_id int,
                airport_id int,
                constraint which_airline foreign key(airline_id) references airline(id) on delete cascade on update cascade,
                constraint at_airport foreign key(airport_id) references airport(id) on delete cascade on update cascade) """, conn)
    print('Flight relation created successfully')