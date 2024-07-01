index_array = ['^NSEBANK', 'NIFTY_FIN_SERVICE.NS', '^CNXENERGY', '^CNXFMCG', '^CNXAUTO', '^CNXMETAL', '^CNXIT',
               '^CNXREALTY', '^CNXPHARMA', '^CNXCONSUM', ]

index = '^CNX100'
import yfinance as yf
import pandas as pd

def normalized_mean(df, on='Close'):
    nz = (df[on] - df[on].mean()) / df[on].std()
    return nz


def normalized_min_max(df, on='Close'):
    nz = (df[on] - df[on].min()) / (df[on].max() - df.min())
    return nz


df = yf.download(tickers=index_array, period='1y', group_by='ticker',keepna=True)

df = df.loc[:, (slice(None), 'Close')]
df.columns = df.columns.get_level_values(0)
df = df.dropna(axis=1, how='all')


bench_mark = yf.download(index, period='1y')

for i in df.columns:
    df[i] = normalized_mean(df, i)
df['nifty'] = normalized_mean(bench_mark, 'Close')
df = df.dropna(how='all')

rrg = pd.DataFrame()
for i in df.columns:
    rrg[f'{i}.rs'] = df['nifty']

for i in df.columns:
    rrg[f'{i}.rsm'] = rrg[f'{i}.rs'].pct_change(fill_method=None)*100


#------------------
import plotly.graph_objects as go

def plot_relative_strength(df, index_name):
  fig = go.Figure()

  fig.add_trace(go.Scatter(
      x=df[f'{index_name}.rs'],
      y=df[f'{index_name}.rsm'],
      mode='lines',
      name=f'Relative Strength of {index_name}'
  ))

  fig.update_layout(
      title=f'Relative Strength of {index_name} vs. Nifty',
      xaxis_title='Date',
      yaxis_title='Relative Strength'
  )

  fig.show()

plot_relative_strength(rrg.copy(), '^CNXCONSUM')