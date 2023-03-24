
import json, time, traceback, os
import pandas as pd
from sqlalchemy import *

"""
************************************  General Workflow with (SQLalchemy)Psycopg2:  ************************************
STEP#0. Test if you can successfully connect to PostgreSQL database:
            test_connection()
STEP#1. Initiate a connection object(conn below) and connect to PostgreSQL database: 
            db = DbConnection()
STEP#2. The connection object has a cursor object(db.cur) as object attribute. Call cursor object's execute 
        method to execute SQL language:
            db.cur.execute(SQL query) 
STEP#3. Commit the SQL language executed by cursor object. Use the connection object() to commit):		
            db.raw_conn.commit()
STEP#4. Return the result of SQL query, the data type is pythong list.
            result = db.cur.fetchall()
"""


def main():
    try:
        # STEP#1 ETL the source data
        etl_source_data()

        # STEP#2 Respectively ETL the crime, date, weather, neighbourhood data
        etl_crime_date_data()
        etl_weather_neighbourhood_data()

        # STEP#3 ETL fact table
        etl_fact_table()

    except:
        print(traceback.format_exc())


def etl_fact_table():
    try:
        test_connection()
        with DbConnection() as db:
            # STEP#1 Remove redundant columns in the fact table
            command1 = """ALTER TABLE crime_weather_source_table
                        DROP COLUMN event_id, DROP COLUMN location_type, DROP COLUMN year,
                        DROP COLUMN month, DROP COLUMN day, DROP COLUMN day_of_year,
                        DROP COLUMN day_of_week, DROP COLUMN crime_type, DROP COLUMN hood_id,
                        DROP COLUMN neighbourhood_name, DROP COLUMN weather;"""
            db.cur.execute(command1)
            db.raw_conn.commit()

            # STEP#2 Add the sum of daily crime number to the fact table
            command2 = """create table fact_table as
                            (select date_surrogate_key, event_surrogate_key, climate_surrogate_key, neighbourhood_surrogate_key,
                            count(date_surrogate_key) over (partition by date_surrogate_key) as crime_number, 
                            temperature_mean, temperature_min, temperature_max from crime_weather_source_table)"""
            db.cur.execute(command2)
            db.raw_conn.commit()

            # STEP#3 Add foreign key constraints to the fact table
            command3 = """ALTER TABLE fact_table ADD PRIMARY KEY 
                            (date_surrogate_key, event_surrogate_key, climate_surrogate_key, neighbourhood_surrogate_key);"""
            db.cur.execute(command3)
            db.raw_conn.commit()

            # STEP#4 Add primary key constraints to the fact table
            add_foreign_keys()

            # STEP#5 Remove redundant table in DBMS
            command4 = """DROP TABLE IF EXISTS crime_weather_source_table"""
            db.cur.execute(command4)
            db.raw_conn.commit()



    except:
        print(traceback.format_exc())


def add_foreign_keys():
    try:
        test_connection()
        with DbConnection() as db:
            # Add foreign key constraints between crime_weather_source_table and climate_surrogate_table
            db.cur.execute(
                "ALTER TABLE fact_table ADD CONSTRAINT climate_foreign_key FOREIGN KEY (climate_surrogate_key) REFERENCES climate_surrogate_table(climate_surrogate_key);")
            db.raw_conn.commit()

            # Add foreign key constraints between crime_weather_source_table and neighbourhood_surrogate_table
            db.cur.execute(
                "ALTER TABLE fact_table ADD CONSTRAINT neighbourhood_foreign_key FOREIGN KEY (neighbourhood_surrogate_key) REFERENCES neighbourhood_surrogate_table(neighbourhood_surrogate_key);")
            db.raw_conn.commit()


            # Add foreign key to crime_weather_source_table referencing date_surrogate_table
            command1 = """alter table fact_table
                            ADD 
                            FOREIGN KEY (date_surrogate_key) 
                            REFERENCES date_surrogate_table (date_surrogate_key)
                            ON DELETE CASCADE"""
            db.cur.execute(command1)
            db.raw_conn.commit()

            # Add foreign key to crime_weather_source_table referencing crime_event_surrogate_table
            command2 = """alter table fact_table
                            ADD 
                            FOREIGN KEY (event_surrogate_key) 
                            REFERENCES crime_event_surrogate_table (event_surrogate_key)
                            ON DELETE CASCADE"""
            db.cur.execute(command2)
            db.raw_conn.commit()

    except:
        print(traceback.format_exc())





def etl_weather_neighbourhood_data():
    try:
        transform_weather_data()
        transform_neighbourhood_data()
    except:
        print(traceback.format_exc())

def transform_weather_data():
    """A function which fetches the crime_weather_source_table data
       and creates the climate_dimension_table and the climate_surrogate_table.
       The function also merges the climate_surrogate_key to the crime_weather_source_table.
    """

    try:
        test_connection()
        with DbConnection() as db:

            # get the data from crime_weather_source_table
            db.cur.execute('SELECT * FROM crime_weather_source_table')
            db.raw_conn.commit()

            crime_weather_lst = db.cur.fetchall()

            # get the columns of crime_weather_source_table
            crime_weather_table_columns = [col[0] for col in list(db.cur.description)]

            # create the dataframe for crime_weather_source_table
            df_crime_weather = pd.DataFrame(crime_weather_lst, columns=crime_weather_table_columns)

            # the columns we want to group by
            selected_columns = ['year', 'month', 'day', 'temperature_min', 'temperature_max', 'weather']

            # the columns we need to build the climate table
            climate_dimension_table_columns = ['day', 'month', 'year', 'temperature_mean', 'temperature_min',
                                               'temperature_max', 'weather']

            df_climate = df_crime_weather.groupby(selected_columns).agg({'temperature_mean': 'min'}).reset_index()
            df_climate['temperature_mean'] = df_climate['temperature_mean'].round()

            df_climate = df_climate[climate_dimension_table_columns]
            df_climate = df_climate.sort_values(['year', 'month', 'day'])

            # change the colums to integer (since it's temperature, the decimal is irrelevant)
            df_climate['temperature_mean'] = df_climate['temperature_mean'].astype(int)
            df_climate['temperature_min'] = df_climate['temperature_min'].astype(int)
            df_climate['temperature_max'] = df_climate['temperature_max'].astype(int)

            # This will be needed for the lookup table
            year_month_day_columns = df_climate[['year', 'month', 'day']]

            # Push to PostgreSQL
            df_climate.to_sql("climate_dimension_table", con=db.engine, if_exists="replace", index=True)

            # Set the composite PKs
            db.cur.execute("ALTER TABLE climate_dimension_table ADD PRIMARY KEY(year,month,day);")
            db.raw_conn.commit()

            # Create the climate_surrogate_table dataframe
            df_climate_lookup = year_month_day_columns

            df_climate_lookup.insert(0, 'climate_surrogate_key', range(1, len(df_climate_lookup) + 1))

            # Set the data to be of type integer
            integer_type_map = {"year": int, "month": int, "day": int, "climate_surrogate_key": int}

            df_climate_lookup = df_climate_lookup.astype(integer_type_map)
            # df_climate_lookup['year'] = df_climate_lookup['year'].astype(int)
            # df_climate_lookup['month'] = df_climate_lookup['month'].astype(int)
            # df_climate_lookup['day'] = df_climate_lookup['day'].astype(int)
            # df_climate_lookup['climate_surrogate_key'] = df_climate_lookup['climate_surrogate_key'].astype(int)

            # Push the dataframe to PostgreSQL
            df_climate_lookup.to_sql("climate_surrogate_table", con=db.engine, if_exists="replace", index=False)

            # Set the composite PKs
            db.cur.execute("ALTER TABLE climate_surrogate_table ADD PRIMARY KEY(climate_surrogate_key);")
            db.raw_conn.commit()

            # Add foreign key constraints between climate_surrogate_table and climate_dimension_table
            db.cur.execute(
                "ALTER TABLE climate_surrogate_table ADD CONSTRAINT climate_foreign_key FOREIGN KEY (year,month,day) REFERENCES climate_dimension_table(year,month,day);")
            db.raw_conn.commit()

            # Left join crime_weather_source_table with climate_surrogate_table
            df_merge = pd.merge(df_crime_weather, df_climate_lookup, on=['year', 'month', 'day'], how='left')

            # Push the merged table to PostgreSQL
            df_merge.to_sql("crime_weather_source_table", con=db.engine, if_exists="replace", index=False)

    except:
        print(traceback.format_exc())


def transform_neighbourhood_data():
    """A function which fetches the crime_weather_source_table data
       and creates the neighbourhood_dimension_table and the neighbourhood_surrogate_table.
       The function also merges the neighbourhood_surrogate_key to the crime_weather_source_table.
    """

    try:
        test_connection()
        with DbConnection() as db:
            # Get the data from crime_weather_source_table
            db.cur.execute('SELECT * FROM crime_weather_source_table')
            db.raw_conn.commit()
            crime_weather_lst = db.cur.fetchall()

            # Get the crime_weather_source_table columns
            crime_weather_table_columns = [col[0] for col in list(db.cur.description)]

            # Create the crime_weather_source_table dataframe
            df_crime_weather = pd.DataFrame(crime_weather_lst, columns=crime_weather_table_columns)

            # Create the neighbourhood_look_up table dataframe
            df_neighbourhood_lookup = pd.DataFrame(columns=['hood_id'])
            df_neighbourhood_lookup['hood_id'] = df_crime_weather['hood_id'].unique()

            # Add the surrogate key column
            df_neighbourhood_lookup.insert(0, 'neighbourhood_surrogate_key', range(1, len(df_neighbourhood_lookup) + 1))

            # Set the data to be of type integer
            integer_type_map = {"hood_id": int}
            df_neighbourhood_lookup = df_neighbourhood_lookup.astype(integer_type_map)

            df_neighbourhood_lookup['hood_id'] = df_neighbourhood_lookup['hood_id'].astype(int)
            df_neighbourhood_lookup['neighbourhood_surrogate_key'] = df_neighbourhood_lookup[
                'neighbourhood_surrogate_key'].astype(int)

            # Push the neighbourhood_surrogate_table to PostgreSQL
            df_neighbourhood_lookup.to_sql("neighbourhood_surrogate_table", con=db.engine, if_exists="replace",
                                           index=False)

            # Set the composite PKs
            db.cur.execute("ALTER TABLE neighbourhood_surrogate_table ADD PRIMARY KEY(neighbourhood_surrogate_key);")
            db.raw_conn.commit()
            # Create the neighbourhood table
            df_neighbourhood = df_crime_weather[['hood_id', 'neighbourhood_name']].drop_duplicates()
            df_neighbourhood = df_neighbourhood.sort_values(['hood_id'])

            # Push the neighbourhood_dimension_table dataframe to PostgreSQL
            df_neighbourhood.to_sql("neighbourhood_dimension_table", con=db.engine, if_exists="replace", index=False)

            # Set the hood_id as the PK
            db.cur.execute("ALTER TABLE neighbourhood_dimension_table ADD PRIMARY KEY(hood_id);")
            db.raw_conn.commit()

            # Add foreign key constraints between neighbourhood_surrogate_table and neighbourhood_dimension_table
            db.cur.execute(
                "ALTER TABLE neighbourhood_surrogate_table ADD CONSTRAINT neighbourhood_foreign_key FOREIGN KEY (hood_id) REFERENCES neighbourhood_dimension_table(hood_id);")
            db.raw_conn.commit()

            # Left join crime_weather_source_table with neighbourhood_surrogate_table
            df_merge = pd.merge(df_crime_weather, df_neighbourhood_lookup, on='hood_id', how='left')
            df_merge.to_sql("crime_weather_source_table", con=db.engine, if_exists="replace", index=False)

    except:
        print(traceback.format_exc())





class DbConnection:
    """
    The class DbConnection is used to create an object to connect database system. the underlying implementation
    of the connection is undertaken by Python library SQLAlchemy with DBAPI Psycopg2.
    Note: the json file named "config.json" is required since it contains all the parameters for database connection.

    Preliminaries:
        packages required: sqlalchemy, json, time.

        files required: config.json.

        files path: the config.json should be in the same directory with this Python file.
    """

    def __init__(self):
        try:
            with open("./config.json") as jsonfile:
                config = json.load(jsonfile)

                database = config["database"]
                user = config["user"]
                password = config["password"]
                host = config["host"]
                port = config["port"]
                dialect = "postgresql"  # dialect is DBMS type, such as mysql, sqlite, postgresql, etc.
                url = f"{dialect}://{user}:{password}@{host}:{port}/{database}"

                # Use sqlalchemy engine configuration to connect the database system.
                self.engine = create_engine(url, pool_size=5, isolation_level="AUTOCOMMIT")
                self.conn = self.engine.connect()
                self.raw_conn = self.engine.raw_connection()  # Raw connection help invoke the connection from DBAPI(Psycopg2)
                self.cur = self.raw_conn.cursor()
                print("Connection to database successfully... \n")

        except Exception as e:
            print(f"Connection to database failed: {e} \n")
        self.start_time = time.time()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.conn.close()
        self.raw_conn.close()
        print(
            f"Released database connection: held for {time.time() - self.start_time} seconds \n")


def test_connection():
    try:
        with DbConnection() as db:
            db.cur.execute("select version();")
            db.raw_conn.commit()
            print(db.cur.fetchall())
    except:
        print(traceback.format_exc())



def etl_source_data():
    try:
        test_connection()
        with DbConnection() as db:

            #STEP#1-1 Data extraction: extract crime source data from crime_dataset.csv file
            df = pd.read_csv("./crime_dataset.csv")
            print(df.head())
            print(df.shape)

            # STEP#2-1 Data transformation(crime data transformation): remove duplicate/noise, handle null, filter data, etc.
            # (1) Unify column name to lower case
            for column_name in df.columns:
                df.rename(columns={column_name: column_name.lower()}, inplace=True)
                print(column_name)

            # (2) Remove duplicated event_id
            df.drop_duplicates(subset=['event_id'], keep='first', inplace=True)

            # (3) Remove noise data which is not in span from year 2017 to 2020
            df = df[df.occurrence_year.isin([2017, 2018, 2019, 2020])]

            # (4) Unify text value to lower case
            df.event_id = df.event_id.str.lower()
            df.occurrence_month = df.occurrence_month.str.lower()
            df.location_type = df.location_type.str.lower()
            df.day_of_week = df.day_of_week.str.lower()
            df.crime_type = df.crime_type.str.lower()
            df.neighbourhood_name = df.neighbourhood_name.str.lower()

            # (5) Convert string month to integer
            month_map = {"january":1, "february":2, "march":3, "april":4, "may":5, "june":6,
                         "july":7, "august":8, "september":9, "october":10, "november":11, "december":12}
            df.occurrence_month = df.occurrence_month.map(month_map)

            # (6) Fix noise data in column neighbourhood_name and hood_id
            df.neighbourhood_name.replace("nsa", "random", inplace=True)
            df.hood_id.replace("NSA", "0", inplace=True)

            # (7) Unify integer value to integer type
            integer_type_map = {"occurrence_year": int, "occurrence_month": int,
                                "occurrence_day": int, "day_of_year": int, "hood_id": int}
            df = df.astype(integer_type_map)

            # (8) Rename column
            df.rename(columns={df.columns[2]: 'year', df.columns[3]: 'month', df.columns[4]: 'day'}, inplace=True)

            # STEP#3-1 Data loading(load crime source data)
            df.to_sql("crime_source_table", con=db.engine, if_exists="replace", index=False)


            # STEP#1-2 Data extraction: extract weather source data from crime_dataset.csv file
            weather_df_list = []
            path = "weather_dataset"
            weather_source_files = os.listdir(path)
            for file_name in weather_source_files:
                weather_source_path = path + "/" + file_name
                df_weather_peryear = pd.read_csv(weather_source_path)
                weather_df_list.append(df_weather_peryear)
            df_weather = pd.concat(weather_df_list, axis=0, ignore_index=True)

            print(df_weather.shape)
            print(df_weather.head)

            # STEP#2-2 Data transformation(weather data transformation): remove duplicate/noise, handle null, filter data, etc.
            # (1) Drop irrelevant columns
            df_weather.drop(columns=["Longitude (x)", "Latitude (y)", "Station Name", "Climate ID", "Date/Time",
                                     "Temp Flag", "Dew Point Temp (°C)", "Dew Point Temp Flag", "Rel Hum (%)",
                                     "Rel Hum Flag", "Wind Dir (10s deg)", "Wind Dir Flag", "Wind Spd (km/h)",
                                     "Wind Spd Flag", "Visibility (km)", "Visibility Flag", "Stn Press (kPa)",
                                     "Stn Press Flag", "Hmdx", "Hmdx Flag", "Wind Chill", "Wind Chill Flag"
                                     ], inplace=True)

            # (2) Rename column
            df_weather.rename(columns={df_weather.columns[4]: 'temperature'}, inplace=True)

            # (3) Unify column name to lower case
            for column_name in df_weather.columns:
                df_weather.rename(columns={column_name: column_name.lower()}, inplace=True)

            # (4) Remove duplicated event_id
            df_weather.drop_duplicates(subset=['year', 'month', 'day', 'time'], keep='first', inplace=True)

            # (5) Unify text value to lower case
            df_weather.weather = df_weather.weather.str.lower()


            # (6) Handle the null value in weahter column
            df_weather.weather.fillna(value="normal", inplace=True)

            # (7) Calculate mean, min, and max temperature
            group_date = df_weather.groupby(['year', 'month', 'day'], as_index=False)
            argg_group_date = group_date.agg( {'temperature': ['mean', 'min', 'max'], 'weather':['max']})
            argg_group_date.columns = list(map(''.join, argg_group_date.columns.values))
            df_weather = argg_group_date
            df_weather.rename(columns={df_weather.columns[3]: 'temperature_mean',
                                       df_weather.columns[4]: 'temperature_min',
                                       df_weather.columns[5]: 'temperature_max',
                                       df_weather.columns[6]: 'weather'}, inplace=True)

            # (8) Unify integer value to integer type
            integer_type_map = {"year": int, "month": int, "day": int}
            df_weather = df_weather.astype(integer_type_map)


            # STEP#3-2 Data loading(load weather data)
            df_weather.to_sql("weather_source_table", con=db.engine, if_exists="append", index=False)


            # STEP#3-3 Data loading(load joint weather & crime data)
            df_merge = pd.merge(df, df_weather, on=['year', 'month', 'day'], how='left')
            df_merge.to_sql("crime_weather_source_table", con=db.engine, if_exists="replace", index=False)


    except:
        print(traceback.format_exc())


def etl_crime_date_data():
    try:
        test_connection()

        with DbConnection() as db:

            # STEP#1 Extract date source data from crime_weather_source_table
            print("CHECK__________________________________________________________")
            db.cur.execute(
                "create table date_source_table as (select year, month, day, day_of_year, day_of_week from crime_weather_source_table)")
            db.raw_conn.commit()

            # STEP#2 Extract date dimension table from date source table by removing duplicate
            db.cur.execute("create table date_dimension_table as (select distinct * from date_source_table)")
            db.raw_conn.commit()

            # STEP#3 Add primary key to date dimension table
            db.cur.execute("alter table date_dimension_table add primary key (year, month, day)")
            db.raw_conn.commit()

            # STEP#4 Generate date surrogate table
            command = """create table date_surrogate_table(
                                date_surrogate_key SERIAL,
                                year int,
                                month int,
                                day int,
                                primary key (date_surrogate_key)
                                )"""
            db.cur.execute(command)

            command2 = """insert into date_surrogate_table(year, month, day)
                            select distinct year, month, day from crime_weather_source_table"""
            db.cur.execute(command2)

            command3 = """select * from crime_weather_source_table join date_surrogate_table
                            using(year, month, day) order by year, month, day ASC LIMIT 10"""
            db.cur.execute(command3)
            result = db.cur.fetchall()
            for i in result:
                print(i)
            db.raw_conn.commit()

            # STEP#5 add date_surrogate_key into tmp_crime_weather_source_table
            command4 = """create table tmp_crime_weather_source_table as
                            (select event_id, location_type, crime_weather_source_table.year, crime_weather_source_table.month, crime_weather_source_table.day, day_of_year, day_of_week, crime_type, hood_id, neighbourhood_name, temperature_mean, temperature_min, temperature_max, weather, date_surrogate_table.date_surrogate_key 
                            from crime_weather_source_table join date_surrogate_table
	                        on crime_weather_source_table.year = date_surrogate_table.year and 
	                        crime_weather_source_table.month = date_surrogate_table.month and 
	                        crime_weather_source_table.day = date_surrogate_table.day)"""
            db.cur.execute(command4)
            db.raw_conn.commit()
            db.cur.execute("drop table crime_weather_source_table")
            db.raw_conn.commit()

            # STEP#6 Extract crime event source data from crime_weather_source_table
            db.cur.execute(
                "create table crime_event_source_table as (select event_id, crime_type, year, month, day, day_of_year, day_of_week, location_type from tmp_crime_weather_source_table)")
            db.raw_conn.commit()

            # STEP#7 Extract crime event dimension table from crime event source table by removing duplicate
            db.cur.execute(
                "create table crime_event_dimension_table as (select distinct * from crime_event_source_table)")
            db.raw_conn.commit()

            # STEP#8 Add primary key to crime event dimension table
            db.cur.execute("alter table crime_event_dimension_table add primary key (event_id)")
            db.raw_conn.commit()

            # STEP#9 Generate crime event surrogate table
            command = """create table crime_event_surrogate_table(
                                event_surrogate_key SERIAL,
                                event_id text,
                                primary key (event_surrogate_key)
                                )"""
            db.cur.execute(command)

            command2 = """insert into crime_event_surrogate_table(event_id)
                            select distinct event_id from tmp_crime_weather_source_table"""
            db.cur.execute(command2)

            command3 = """select * from tmp_crime_weather_source_table join crime_event_surrogate_table
                            using(event_id) order by event_id ASC LIMIT 10"""
            db.cur.execute(command3)
            result = db.cur.fetchall()
            for i in result:
                print(i)
            db.raw_conn.commit()

            # STEP#10 add crime_event_surrogate_key into crime_weather_source_table
            command4 = """create table crime_weather_source_table as
                            (select tmp_crime_weather_source_table.event_id, location_type, year, month, day, day_of_year, day_of_week, crime_type, hood_id, neighbourhood_name, temperature_mean, temperature_min, temperature_max, weather, date_surrogate_key, event_surrogate_key
                            from tmp_crime_weather_source_table join crime_event_surrogate_table
	                        on tmp_crime_weather_source_table.event_id = crime_event_surrogate_table.event_id)"""
            db.cur.execute(command4)
            db.raw_conn.commit()
            db.cur.execute("drop table tmp_crime_weather_source_table")
            db.raw_conn.commit()

            # STEP#11
            # Add foreign key to date_surrogate_table referencing date_dimenson_table
            command5 = """alter table date_surrogate_table
                            ADD 
                            FOREIGN KEY (year, month, day) 
                            REFERENCES date_dimension_table (year, month, day)
                            ON DELETE CASCADE"""
            db.cur.execute(command5)
            db.raw_conn.commit()



            # Add foreign key to crime_event_surrogate_table referencing crime_event_dimension_table
            command7 = """alter table crime_event_surrogate_table
                            ADD 
                            FOREIGN KEY (event_id) 
                            REFERENCES crime_event_dimension_table (event_id)
                            ON DELETE CASCADE"""
            db.cur.execute(command7)
            db.raw_conn.commit()

    except:
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
