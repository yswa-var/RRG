import calendar
from datetime import datetime, timedelta
import webbrowser
import pandas as pd
from fyers_apiv3 import fyersModel
from settings import passes_instance as passwords

class DataDownloader:
    def __init__(self):
        self.client_id = passwords.sid
        self.redirect_uri = passwords.url
        self.secret_key = passwords.key
        self.response_type = "code"
        self.grant_type = "authorization_code"
        self.state = "sample"
        self.fyers = None

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

    def generate_ticker_list(self):
        nse_ticker = pd.read_csv('nifty200.csv')
        nse_ticker['Symbol'] = 'NSE:' + nse_ticker['Symbol'] + '-EQ'
        return nse_ticker['Symbol'].tolist()

    def get_date_range(self, days=252):
        date_to = calendar.timegm(datetime.now().timetuple())
        date_from = calendar.timegm((datetime.now() - timedelta(days)).timetuple())
        return date_from, date_to

    def download_data(self, symbol, date_from, date_to):
        if not self.fyers:
            raise ValueError("Authentication required. Call authenticate() first.")

        data = {
            "symbol": symbol,
            "resolution": "D",
            "date_format": "1",
            "range_from": date_from,
            "range_to": date_to,
            "cont_flag": "1"
        }

        response = self.fyers.history(data)
        if response['s'] == 'ok':
            return pd.DataFrame(response['candles'], columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        else:
            print(f"Error downloading data for {symbol}: {response['message']}")
            return None

    def download_all_data(self):
        self.authenticate()
        symbols = self.generate_ticker_list()
        date_from, date_to = self.get_date_range()

        all_data = {}
        for symbol in symbols:
            data = self.download_data(symbol, date_from, date_to)
            if data is not None:
                all_data[symbol] = data

        return all_data


if __name__ == "__main__":
    downloader = DataDownloader()
    data = downloader.download_all_data()
    print(f"Downloaded data for {len(data)} symbols")