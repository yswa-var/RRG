# import psycopg2
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
# from sqlalchemy import create_engine, Column, Integer, Float, MetaData, Table, VARCHAR, TIMESTAMP, BigInteger
#
# # Database connection details
# PG_HOST = 'localhost'
# PG_USER = 'postgres'
# PG_PASSWORD = '12345'
# PG_DATABASE = 'nifty200'
#
#
# def create_database_if_not_exists(dbname):
#     # Connect to PostgreSQL server
#     conn = psycopg2.connect(
#         host=PG_HOST,
#         user=PG_USER,
#         password=PG_PASSWORD
#     )
#     conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
#
#     # Create a cursor object
#     cur = conn.cursor()
#
#     # Check if database exists
#     cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{dbname}'")
#     exists = cur.fetchone()
#
#     if not exists:
#         cur.execute(f"CREATE DATABASE {dbname}")
#         print(f"Database '{dbname}' created successfully.")
#     else:
#         print(f"Database '{dbname}' already exists.")
#
#     # Close cursor and connection
#     cur.close()
#     conn.close()
#
#
# # Create the database if it doesn't exist
# create_database_if_not_exists(PG_DATABASE)
#
# # Create the database engine
# engine = create_engine(f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}/{PG_DATABASE}')
#
# # Create a MetaData instance
# metadata = MetaData()
#
# # Define the table
# ohlc_data = Table(
#     'ohlc_data', metadata,
#     Column('id', BigInteger, primary_key=True, autoincrement=True),
#     Column('timestamp', TIMESTAMP(False), nullable=False),
#     Column('symbol', VARCHAR(200), nullable=False),
#     Column('open', Float),
#     Column('high', Float),
#     Column('low', Float),
#     Column('close', Float),
#     Column('volume', BigInteger),
# )
#
# # Create the table in the database
# metadata.create_all(engine)
#
# print("Table 'ohlc_data' has been created successfully.")

from db_schema import DatabaseManager

db_manager = DatabaseManager()

db_manager.create_ohlc_table()
