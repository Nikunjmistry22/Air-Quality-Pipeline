import psycopg2
from psycopg2 import pool
from constants import DATABASE_URI
import pandas as pd

def create_connection_pool():
    """Creates a connection pool."""
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, DATABASE_URI)
        if connection_pool:
            print("Connection pool created successfully")
        return connection_pool
    except Exception as e:
        print("Error creating connection pool:", e)
        return None

def create_tables_if_not_exist(connection, schema):
    """Creates the necessary dimension and fact tables if they do not already exist."""
    try:
        cursor = connection.cursor()

        create_tables_query = f"""
        -- City Dimension Table
        CREATE TABLE IF NOT EXISTS {schema}.city_dimension (
            city_id SERIAL PRIMARY KEY,
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100),
            latitude DECIMAL,
            longitude DECIMAL
        );

        -- Time Dimension Table
        CREATE TABLE IF NOT EXISTS {schema}.time_dimension (
            time_id SERIAL PRIMARY KEY,
            date DATE,
            year INT,
            quarter INT,
            month INT,
            week_number INT,
            day INT,
            day_of_week INT
        );

        -- Weather Dimension Table
        CREATE TABLE IF NOT EXISTS {schema}.weather_dimension (
            weather_id SERIAL PRIMARY KEY,
            temperature DECIMAL,
            pressure DECIMAL,
            humidity DECIMAL,
            wind_speed DECIMAL,
            wind_direction VARCHAR(100),
            icon VARCHAR(50)
        );

        -- Pollution Dimension Table
        CREATE TABLE IF NOT EXISTS {schema}.pollution_dimension (
            pollution_id SERIAL PRIMARY KEY,
            aqius DECIMAL,
            mainus VARCHAR(50),
            aqicn DECIMAL,
            maincn VARCHAR(50)
        );

        -- Weather Fact Table
        CREATE TABLE IF NOT EXISTS {schema}.weather_fact (
            weather_fact_id SERIAL PRIMARY KEY,
            city_id INT REFERENCES {schema}.city_dimension(city_id),
            time_id INT REFERENCES {schema}.time_dimension(time_id),
            weather_id INT REFERENCES {schema}.weather_dimension(weather_id),
            weather_ts TIMESTAMP,
            date_fetched TIMESTAMP
        );

        -- Pollution Fact Table
        CREATE TABLE IF NOT EXISTS {schema}.pollution_fact (
            pollution_fact_id SERIAL PRIMARY KEY,
            city_id INT REFERENCES {schema}.city_dimension(city_id),
            time_id INT REFERENCES {schema}.time_dimension(time_id),
            pollution_id INT REFERENCES {schema}.pollution_dimension(pollution_id),
            pollution_ts TIMESTAMP,
            date_fetched TIMESTAMP
        );
        """
        cursor.execute(create_tables_query)
        connection.commit()
        print("Tables created or already exist.")
    except Exception as e:
        print("Error creating tables:", e)
    finally:
        cursor.close()

def insert_city_dimension(connection, df, schema):
    """Insert city data into city_dimension table and return a map of city information to city_id."""
    city_id_map = {}
    try:
        cursor = connection.cursor()
        # Get existing cities from the database
        cursor.execute(f"SELECT city, state, country, city_id FROM {schema}.city_dimension;")
        existing_cities = cursor.fetchall()
        existing_city_map = {(city, state, country): city_id for city, state, country, city_id in existing_cities}

        for _, row in df[['city', 'state', 'country', 'latitude', 'longitude']].drop_duplicates().iterrows():
            city_key = (row['city'], row['state'], row['country'])
            if city_key in existing_city_map:
                # If the city already exists, map its city_id
                city_id_map[city_key] = existing_city_map[city_key]
            else:
                # Insert new city and get the city_id
                insert_query = f"""
                INSERT INTO {schema}.city_dimension (city, state, country, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING city_id;
                """
                cursor.execute(insert_query, (row['city'], row['state'], row['country'], row['latitude'], row['longitude']))
                city_id = cursor.fetchone()
                if city_id:
                    city_id_map[city_key] = city_id[0]  # Store the city_id
        connection.commit()
        print("City data inserted successfully or found existing cities.")
    except Exception as e:
        print("Error inserting city data:", e)
    finally:
        cursor.close()
    return city_id_map

def insert_time_dimension(connection, df, schema):
    """Insert time data into time_dimension table and return a map of date to time_id."""
    time_id_map = {}
    try:
        cursor = connection.cursor()
        
        # Extract date components from the timestamp
        df['date'] = pd.to_datetime(df['weather_ts']).dt.date
        df['year'] = pd.to_datetime(df['weather_ts']).dt.year
        df['quarter'] = pd.to_datetime(df['weather_ts']).dt.quarter
        df['month'] = pd.to_datetime(df['weather_ts']).dt.month
        df['week_number'] = pd.to_datetime(df['weather_ts']).dt.isocalendar().week
        df['day'] = pd.to_datetime(df['weather_ts']).dt.day
        df['day_of_week'] = pd.to_datetime(df['weather_ts']).dt.dayofweek

        for _, row in df.drop_duplicates(subset=['date', 'year', 'quarter', 'month', 'week_number', 'day', 'day_of_week']).iterrows():
            insert_query = f"""
            INSERT INTO {schema}.time_dimension (date, year, quarter, month, week_number, day, day_of_week)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING time_id;
            """
            cursor.execute(insert_query, (row['date'], row['year'], row['quarter'], row['month'], row['week_number'], row['day'], row['day_of_week']))
            time_id = cursor.fetchone()
            if time_id:
                time_id_map[row['date']] = time_id[0]  # Store the time_id for the date
        connection.commit()
        print("Time data inserted successfully.")
    except Exception as e:
        print("Error inserting time data:", e)
    finally:
        cursor.close()
    return time_id_map

def insert_weather_dimension(connection, df, schema):
    """Insert weather data into weather_dimension table and return a map of weather information to weather_id."""
    weather_id_map = {}
    try:
        cursor = connection.cursor()
        for _, row in df[['temperature', 'pressure', 'humidity', 'wind_speed', 'wind_direction', 'icon']].drop_duplicates().iterrows():
            insert_query = f"""
            INSERT INTO {schema}.weather_dimension (temperature, pressure, humidity, wind_speed, wind_direction, icon)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING weather_id;
            """
            cursor.execute(insert_query, (
                row['temperature'], row['pressure'], row['humidity'], row['wind_speed'], row['wind_direction'], row['icon']
            ))
            weather_id = cursor.fetchone()
            if weather_id:
                weather_key = (row['temperature'], row['pressure'], row['humidity'], row['wind_speed'], row['wind_direction'], row['icon'])
                weather_id_map[weather_key] = weather_id[0]  # Store the weather_id
        connection.commit()
        print("Weather data inserted successfully.")
    except Exception as e:
        print("Error inserting weather data:", e)
    finally:
        cursor.close()
    return weather_id_map

def insert_pollution_dimension(connection, df, schema):
    """Insert pollution data into pollution_dimension table and return a map of pollution information to pollution_id."""
    pollution_id_map = {}
    try:
        cursor = connection.cursor()
        for _, row in df[['aqius', 'mainus', 'aqicn', 'maincn']].drop_duplicates().iterrows():
            insert_query = f"""
            INSERT INTO {schema}.pollution_dimension (aqius, mainus, aqicn, maincn)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING pollution_id;
            """
            cursor.execute(insert_query, (
                row['aqius'], row['mainus'], row['aqicn'], row['maincn']
            ))
            pollution_id = cursor.fetchone()
            if pollution_id:
                pollution_key = (row['aqius'], row['mainus'], row['aqicn'], row['maincn'])
                pollution_id_map[pollution_key] = pollution_id[0]  # Store the pollution_id
        connection.commit()
        print("Pollution data inserted successfully.")
    except Exception as e:
        print("Error inserting pollution data:", e)
    finally:
        cursor.close()
    return pollution_id_map

def insert_weather_fact(connection, df, city_id_map, time_id_map, weather_id_map, schema):
    """Insert data into the weather_fact table."""
    try:
        cursor = connection.cursor()
        for _, row in df.iterrows():
            city_key = (row['city'], row['state'], row['country'])
            time_id = time_id_map.get(row['date'])
            weather_key = (row['temperature'], row['pressure'], row['humidity'], row['wind_speed'], row['wind_direction'], row['icon'])

            if city_key in city_id_map and time_id and weather_key in weather_id_map:
                insert_query = f"""
                INSERT INTO {schema}.weather_fact (city_id, time_id, weather_id, weather_ts, date_fetched)
                VALUES (%s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, (
                    city_id_map[city_key], time_id, weather_id_map[weather_key], row['weather_ts'], row['date_fetched']
                ))
        connection.commit()
        print("Weather fact data inserted successfully.")
    except Exception as e:
        print("Error inserting weather fact data:", e)
    finally:
        cursor.close()

def insert_pollution_fact(connection, df, city_id_map, time_id_map, pollution_id_map, schema):
    """Insert data into the pollution_fact table."""
    try:
        cursor = connection.cursor()
        for _, row in df.iterrows():
            city_key = (row['city'], row['state'], row['country'])
            time_id = time_id_map.get(row['date'])
            pollution_key = (row['aqius'], row['mainus'], row['aqicn'], row['maincn'])

            if city_key in city_id_map and time_id and pollution_key in pollution_id_map:
                insert_query = f"""
                INSERT INTO {schema}.pollution_fact (city_id, time_id, pollution_id, pollution_ts, date_fetched)
                VALUES (%s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, (
                    city_id_map[city_key], time_id, pollution_id_map[pollution_key], row['pollution_ts'], row['date_fetched']
                ))
        connection.commit()
        print("Pollution fact data inserted successfully.")
    except Exception as e:
        print("Error inserting pollution fact data:", e)
    finally:
        cursor.close()

def load_data(df, schema='public'):
    """Main function to load data into the database from a DataFrame."""
    connection_pool = create_connection_pool()

    if connection_pool:
        connection = connection_pool.getconn()
        if connection:
            print("Successfully obtained a connection from the pool")
            create_tables_if_not_exist(connection, schema)
            
            # Insert into dimension tables
            city_id_map = insert_city_dimension(connection, df, schema)
            time_id_map = insert_time_dimension(connection, df, schema)
            weather_id_map = insert_weather_dimension(connection, df, schema)  # Insert weather dimension
            pollution_id_map = insert_pollution_dimension(connection, df, schema)  # Insert pollution dimension
            
            # Insert into fact tables
            insert_weather_fact(connection, df, city_id_map, time_id_map, weather_id_map, schema)  # Populate weather facts
            insert_pollution_fact(connection, df, city_id_map, time_id_map, pollution_id_map, schema)  # Populate pollution facts
            
            connection_pool.putconn(connection)
            print("Data loaded successfully.")
        else:
            print("Failed to obtain a connection")
    else:
        print("Connection pool not created")

