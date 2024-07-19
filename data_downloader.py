import calendar
import time
from datetime import datetime, timedelta
import webbrowser
import pandas as pd
from fyers_apiv3 import fyersModel

from db.db_ops_local import DatabaseManager
from settings import passes_instance as passwords
from tqdm import tqdm


def generate_ticker_list():
    nse_ticker = pd.read_csv('tickers/nifty200.csv')
    nse_ticker['fyers_symbol'] = 'NSE:' + nse_ticker['Symbol'] + '-EQ'
    return nse_ticker


def get_date_range(days=252):
    now = datetime.now()
    date_to = now.strftime("%Y-%m-%d")
    date_from = (now - timedelta(days=252)).strftime("%Y-%m-%d")
    return date_from, date_to


class DataDownloader:
    def __init__(self, db):
        self.client_id = passwords.sid
        self.redirect_uri = passwords.url
        self.secret_key = passwords.key
        self.response_type = "code"
        self.grant_type = "authorization_code"
        self.state = "sample"
        self.fyers = None
        self.db = db

    def authenticate(self):
        session = fyersModel.SessionModel(
            client_id=self.client_id,
            redirect_uri=self.redirect_uri,
            response_type=self.response_type,
            state=self.state,
            secret_key=self.secret_key,
            grant_type=self.grant_type
        )

        auth_link = session.generate_authcode()
        webbrowser.open(auth_link, new=1)
        self.auth_code = input()

        session.set_token(self.auth_code)
        response = session.generate_token()
        access_token = response['access_token']

        self.fyers = fyersModel.FyersModel(token=access_token, is_async=False, client_id=self.client_id, log_path="")

    def download_data(self, symbol, date_from, date_to):
        if not self.fyers:
            raise ValueError("Authentication required. Call authenticate() first.")

        data = {
            "symbol": 'NSE:' + symbol + '-EQ',
            "resolution": "D",
            "date_format": "1",
            "range_from": date_from,
            "range_to": date_to,
            "cont_flag": "1"
        }

        df = self.fyers.history(data)
        try:
            candles = df['candles']
            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            return df
        except:
            print(f"Error downloading data for {symbol}:")

    def download_all_data(self):
        self.authenticate()
        tickers = generate_ticker_list()
        date_from, date_to = get_date_range()

        total_tickers = len(tickers)
        successful_downloads = 0
        start_time = time.time()

        with tqdm(total=total_tickers, desc="Downloading", unit="ticker") as pbar:
            for idx, row in tickers.iterrows():
                symbol = row.iloc[2]
                industry = row.iloc[1]
                df = self.download_data(symbol, date_from, date_to)
                if not df.empty:
                    successful_downloads += 1
                    self.db.upload_dataframe(df, industry, symbol)
                    pbar.set_postfix({"Success": f"{successful_downloads}/{total_tickers}"})
                pbar.update(1)
            df = self.download_data(symbol='NSE:NIFTY500-INDEX', date_from=date_from, date_to=date_to)
            if not df.empty:
                successful_downloads += 1
                self.db.upload_dataframe(df, industry='index', symbol='NIFTY500')
                pbar.set_postfix({"Success": f"{successful_downloads}/{total_tickers}"})

        end_time = time.time()
        total_time = end_time - start_time

        print(f"\nDownload complete!")
        print(f"Successfully downloaded {successful_downloads} out of {total_tickers} tickers.")
        print(f"Total time taken: {total_time:.2f} seconds")


if __name__ == "__main__":
    db = DatabaseManager()
    db.truncket_db()
    downloader = DataDownloader(db=db)
    downloader.download_all_data()
