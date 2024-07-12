import pandas as pd


def get_tickers():
    nse_ticker = pd.read_csv('tickers/nifty200.csv')
    nse_ticker['fyers_symbol'] = 'NSE:' + nse_ticker['Symbol'] + '-EQ'
    return nse_ticker


class EquiWeightIndex:
    def __init__(self, db):
        self.db = db
        self.tickers = get_tickers()

    def calculate_industry_indices(self):
        industry_groups = self.tickers.groupby('Industry')

        industry_indices = {}

        for industry, group in industry_groups:

            industry_data = []
            for _, row in group.iterrows():
                symbol = row['Symbol']
                df = self.db.download(symbol)
                if not df.empty:
                    df['symbol'] = symbol
                    industry_data.append(df)

            if not industry_data:
                print(f"No data available for {industry}")
                continue

            industry_df = pd.concat(industry_data, axis=0)
            industry_df['date'] = pd.to_datetime(industry_df['timestamp']).dt.date
            industry_df['return'] = industry_df.groupby('symbol')['close'].pct_change()
            industry_returns = industry_df.groupby('date')['return'].mean().reset_index()
            industry_returns['index'] = (1 + industry_returns['return']).cumprod()
            industry_indices[industry] = industry_returns[['date', 'index']]

        index_df = pd.DataFrame()
        for industry, data in industry_indices.items():
            if index_df.empty:
                index_df = data.rename(columns={'index': industry})
            else:
                index_df = pd.merge(index_df, data, on='date', how='outer', suffixes=('', f'_{industry}'))
                index_df = index_df.rename(columns={'index': industry})

        if not index_df.empty:
            index_df = index_df.sort_values('date')
            index_df = index_df.ffill()
        else:
            print("No data available for any industry")

        return index_df
