import psycopg2 as pg
import json, time, traceback
import pandas as pd
from sqlalchemy import *
"""
************************************  General Workflow with (SQLalchemy)Psycopg2:  ************************************
STEP#0. Test if you can successfully connect to PosgreSQL database:
        test_connection()
STEP#1. Initiate a connection object(conn below) and connect to PostgreSQL database: 
        db = DbConnection()
STEP#2. The connection object has a cursor object(db.cur) as object attribute. Call cursor object's execute 
   method to execute SQL language:
        db.cur.execute(SQL LANGUAGE)
STEP#3. Commit the SQL language executed by cursor object. Use the connection object() to commit):		
        db.raw_conn.commit()
STEP#4. Return the result of SQL language, the data type is pythong list.
        result = db.cur.fetchall()
"""




def main():
    try:
        """ ***************** Example - General Workflow with (SQLalchemy)Psycopg2 ***************** """
        # STEP#0 Test database connection. Successful if your PostgreSQL version can be printed in the console.
        test_connection()

        # STEP#1 Use with...as to initiate a connection object named db.
        with DbConnection() as db:
            # STEP#2 Use cursor the method execute() of the curosr object(db.cur) to execute SQL language.
            db.cur.execute("select * from test")

            # STEP#3 Commit the SQL language execution
            db.raw_conn.commit()

            # STEP#4 Return the result of SQL language execution
            result = db.cur.fetchall()
            print(result)

        """ ***************** Example - Using Pandas to read csv file and load data into database ***************** """
        # with DbConnection() as db:
        #     # df = pd.read_csv("./en_climate_hourly_ON_6158359_01-2017_P1H.csv", dtype="str")
        #     df = pd.read_csv("./en_climate_hourly_ON_6158359_01-2017_P1H.csv")
        #     print(df.head())
        #     print(df.shape)
        #
        #     for column_name in df.columns:
        #         df.rename(columns={column_name: column_name.lower()}, inplace=True)
        #         print(column_name)
        #     # df = df.astype("str")
        #     df.to_sql("weather_table", con=db.engine, if_exists="replace")

        """ ***************** Example - Create a surrogate table ***************** """
            # # Generate surrogate table
            # command = """create table neighbourhood_table(
            #                     neighbourhood_id SERIAL,
            #                     neighbourhood text,
            #                     primary key (neighbourhood_id)
            #                     )"""
            # db.cur.execute(command)
            # command2 = """insert into neighbourhood_table(neighbourhood)
            #                 select distinct neighbourhood from crimetable"""
            # db.cur.execute(command2)
            #
            # command3 = """select * from crimetable join neighbourhood_table
            #                 using(neighbourhood) order by hood_id ASC LIMIT 10"""
            # db.cur.execute(command3)
            # result = db.cur.fetchall()
            # for i in result:
            #     print(i)
            # db.raw_conn.commit()

            # with DbConnection() as db:
            #     db.cur.execute(
            #         "select * from test")
            #     db.raw_conn.commit()
            #     result = db.cur.fetchall()
            #     print(result)
            # import_csv("tmptable", "./Climate Change_Earth Surface Temperature Data/GlobalLandTemperaturesByMajorCity_UTF.csv")
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








def import_csv(table_name, file_path, schema_name="public"):
    """
        Use PostgreSQL 
        Table must exist before import_csv. Please create your database table before calling import_csv method.
    """

    csv_start_time = time.time()
    try:
        with DbConnection() as db:
            with open(file_path, "r") as file:
                query = f"COPY {schema_name}.{table_name} FROM STDIN WITH DELIMITER ',' NULL '..' CSV HEADER"
                db.cur.copy_expert(query, file)
                db.raw_conn.commit()

        print(f"SUCCESS: Imported data from {file_path} into {schema_name}.{table_name}")
        cost_time = time.time() - csv_start_time
        print(f"Time cost for csv import: {cost_time} seconds \n")  # Calculate the whole time of import
    except Exception as e:
        print(f"FAIL: Error while importing CSV data into database: {e} \n")
        print(traceback.format_exc())









if __name__ == "__main__":
    main()