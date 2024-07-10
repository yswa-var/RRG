from fyers_apiv3 import fyersModel
import webbrowser
import pandas as pd
import calendar
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv, dotenv_values

load_dotenv()
"""
1. Input parameters
"""
redirect_uri = os.getenv('URL')
client_id = os.getenv('ID')
secret_key = os.getenv('KEY')
grant_type = "authorization_code"
response_type = "code"
state = "sample"

appSession = fyersModel.SessionModel(client_id=client_id, redirect_uri=redirect_uri, response_type=response_type,
                                     state=state, secret_key=secret_key, grant_type=grant_type)

generateTokenUrl = appSession.generate_authcode()

print(generateTokenUrl)
webbrowser.open(generateTokenUrl, new=1)
auth_code = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhcGkubG9naW4uZnllcnMuaW4iLCJpYXQiOjE3MjA2MTExODEsImV4cCI6MTcyMDY0MTE4MSwibmJmIjoxNzIwNjEwNTgxLCJhdWQiOlsiZDoxIiwiZDoyIl0sInN1YiI6ImF1dGhfY29kZSIsImRpc3BsYXlfbmFtZSI6IlhZMDMxODgiLCJvbXMiOiJLMSIsImhzbV9rZXkiOm51bGwsIm5vbmNlIjoiIiwiYXBwX2lkIjoiOUM5M09FOEZJQiIsInV1aWQiOiI0OTk4YmM1ZmQ4Yzg0ZWQzODY0ZGFmZjE1MWFlOGJkYSIsImlwQWRkciI6IjE4Mi43MC4xMTUuNDEsIDE3Mi42OS45NC4yMDkiLCJzY29wZSI6IiJ9.IzKtNAXjnpEt5nT9oIFrgnHMbokpq3fB-e-YKMRvEZw'
appSession.set_token(auth_code)
response = appSession.generate_token()
access_token = response['access_token']
fyers = fyersModel.FyersModel(token=access_token, is_async=False, client_id=client_id, log_path="")

nse_ticker = pd.read_csv('tickers/nifty200.csv')
nse_ticker['Symbol'] = 'NSE:' + nse_ticker['Symbol'] + '-EQ'

date_from = calendar.timegm((datetime.now() - timedelta(252)).timetuple())
date_to = calendar.timegm((datetime.now()).timetuple())
