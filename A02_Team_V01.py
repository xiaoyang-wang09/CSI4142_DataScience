
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
        """ ***************** Example - General Workflow with (SQLalchemy)Psycopg2 ***************** """
        # # STEP#0 Test database connection. Successful if your PostgreSQL version can be printed in the console.
        # test_connection()
        #
        # # STEP#1 Use with...as to initiate a connection object named db.
        # with DbConnection() as db:
        #     # STEP#2 Use cursor the method execute() of the curosr object(db.cur) to execute SQL language.
        #     db.cur.execute("select * from test")
        #
        #     # STEP#3 Commit the SQL language execution
        #     db.raw_conn.commit()
        #
        #     # STEP#4 Return the result of SQL language execution
        #     result = db.cur.fetchall()
        #     print(result)

        """ ***************** Example - Using Pandas to read csv file and load data into database ***************** """
        # with DbConnection() as db:
        #     df = pd.read_csv("./en_climate_hourly_ON_6158359_01-2017_P1H.csv")
        #     print(df.head())
        #     print(df.shape)
        #
        #     for column_name in df.columns:
        #         df.rename(columns={column_name: column_name.lower()}, inplace=True)
        #         print(column_name)
        #     # df = df.astype("str")
        #
        #     df.to_sql("weather_table", con=db.engine, if_exists="replace")

        """ ***************** Example - Create a surrogate table ***************** """
        # Generate surrogate table
        # with DbConnection() as db:
        #     command = """create table neighbourhood_table(
        #                         neighbourhood_id SERIAL,
        #                         hood_id text,
        #                         primary key (neighbourhood_id)
        #                         )"""
        #     db.cur.execute(command)
        #
        #     command2 = """insert into neighbourhood_table(hood_id)
        #                     select distinct hood_id from crimetable"""
        #     db.cur.execute(command2)
        #
        #     command3 = """select * from crimetable join neighbourhood_table
        #                     using(hood_id) order by hood_id ASC LIMIT 10"""
        #     db.cur.execute(command3)
        #     result = db.cur.fetchall()
        #     for i in result:
        #         print(i)
        #     db.raw_conn.commit()


        data_extraction()
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



def data_extraction():
    try:
        test_connection()
        with DbConnection() as db:

            #STEP#1 Extract crime source data from crime_dataset.csv file
            df = pd.read_csv("./crime_dataset.csv")
            print(df.head())
            print(df.shape)

            # STEP#2 Clean crime source data (remove duplicate, filter data etc.)
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

            # (9) Load the clean crime data into DBMS
            df.to_sql("crime_source_table", con=db.engine, if_exists="replace", index=False)



            # STEP#3 Extract weather source data from crime_dataset.csv file
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

            # STEP#4 Clean weather source data (remove duplicate, filter data etc.)
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
            df_weather.weather.fillna(value="UNKNOWN", inplace=True)

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

            # (9) Smooth the value and handle the null value in the weather column
            iteration_time = 8
            for iteration in range(iteration_time):
                weather_list = df_weather.weather.tolist()
                for row_number in range(len(weather_list)):
                    current_weather = weather_list[row_number]
                    if row_number==0 and current_weather == "UNKNOWN":
                        df_weather.loc[[row_number],["weather"]] = weather_list[row_number+1]
                    elif row_number == df_weather.shape[0]-1 and current_weather == "UNKNOWN":
                        df_weather.loc[[row_number], ["weather"]] = weather_list[row_number-1]
                    elif row_number!=0 and current_weather == "UNKNOWN":
                        previous_weather = weather_list[row_number-1]
                        after_weather = weather_list[row_number+1]
                        if previous_weather != "UNKNOWN":
                            df_weather.loc[[row_number], ["weather"]] = previous_weather
                        elif after_weather != "UNKNOWN":
                            df_weather.loc[[row_number], ["weather"]] = after_weather

            # (10) Load the clean weather data into DBMS
            df_weather.to_sql("weather_source_table", con=db.engine, if_exists="append", index=False)

            # (11) Left join crime source table with weather source table
            df_merge = pd.merge(df, df_weather, on=['year', 'month', 'day'], how='left')

            # # STEP#5 Load merged source data to DBMS
            df_merge.to_sql("crime_weather_source_table", con=db.engine, if_exists="replace", index=False)


    except:
        print(traceback.format_exc())




if __name__ == "__main__":
    main()