import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, Column, Float, MetaData, Table, VARCHAR, TIMESTAMP, BigInteger, text
import pandas as pd


class DatabaseManager:
    """
    This class manages database connections, table creation, and data upload for the 'nifty200' database.
    """

    def __init__(self):
        self.PG_HOST = 'localhost'
        self.PG_USER = 'postgres'
        self.PG_PASSWORD = '12345'
        self.PG_DATABASE = 'nifty200'
        self.engine = None

    def connect_to_database(self):
        """
        Connects to the PostgreSQL database and creates the engine.
        """
        conn = psycopg2.connect(
            host=self.PG_HOST,
            user=self.PG_USER,
            password=self.PG_PASSWORD
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{self.PG_DATABASE}'")
        exists = cur.fetchone()

        if not exists:
            cur.execute(f"CREATE DATABASE {self.PG_DATABASE}")
            print(f"Database '{self.PG_DATABASE}' created successfully.")
        else:
            print(f"Database '{self.PG_DATABASE}' already exists.")
        cur.close()

        self.engine = create_engine(
            f'postgresql+psycopg2://{self.PG_USER}:{self.PG_PASSWORD}@{self.PG_HOST}/{self.PG_DATABASE}')
        conn.close()
    def truncket_db(self):
        if self.engine is None:
            self.connect_to_database()
        if self.engine is None:
            raise ValueError("Database engine is not initialized. Call 'connect_to_database' first.")
        with self.engine.connect() as conn:
            conn.execute(text('TRUNCATE TABLE ohlc_data'))
            conn.commit()
            print("Table 'ohlc_data' truncated successfully.")

    def create_ohlc_table(self):
        """
        Creates the 'ohlc_data' table if it doesn't exist in the database.
        """

        if self.engine is None:
            self.connect_to_database()

        metadata = MetaData()
        ohlc_data = Table(
            'ohlc_data', metadata,
            Column('id', BigInteger, primary_key=True, autoincrement=True),
            Column('timestamp', TIMESTAMP(False), nullable=False),
            Column('industry', VARCHAR(100), nullable=False),
            Column('symbol', VARCHAR(200), nullable=False),
            Column('open', Float),
            Column('high', Float),
            Column('low', Float),
            Column('close', Float),
            Column('volume', BigInteger),
        )

        metadata.create_all(self.engine)

        print("Table 'ohlc_data' has been created successfully.")

    def upload_dataframe(self, df, industry, symbol):
        """
        Uploads a DataFrame to the 'ohlc_data' table.

        :param df: pandas DataFrame containing OHLC data
        :param industry: string representing the industry for all rows in the DataFrame
        """
        if self.engine is None:
            self.connect_to_database()



        # Prepare the data
        df['symbol'] = symbol
        df['industry'] = industry
        df = df.rename(columns={
            'date': 'timestamp',
            'symbol': 'symbol',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        })

        # Upload the data
        df.to_sql('ohlc_data', self.engine, if_exists='append', index=False)

    def bulk_upload_dataframes(self, data_dict):
        """
        Uploads multiple DataFrames to the 'ohlc_data' table.

        :param data_dict: dictionary where keys are industries and values are DataFrames
        """
        for industry, df in data_dict.items():
            self.upload_dataframe(df, industry)

    def download(self, symbol):
        """
        Get data from db for a particular stock

        :param symbol: The stock symbol to retrieve data for
        :return: A pandas DataFrame containing the stock data
        """
        if self.engine is None:
            self.connect_to_database()

        query = text(f"""
            SELECT *
            FROM ohlc_data
            WHERE symbol = :symbol
            ORDER BY timestamp;
        """)

        df = pd.read_sql_query(query, self.engine, params={'symbol': symbol})
        return df