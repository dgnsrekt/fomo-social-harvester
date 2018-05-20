from models.telegram import Telegram
import plotly
import plotly.offline as py
import plotly.graph_objs as go

import pandas as pd
import numpy as np


df = Telegram.query_all()
df.reset_index(inplace=True)
# df.groupby(['date', 'name'])['mean'].sum().groupby(level='name').mean()
numeric_columns = ['mean', 'median', 'sum']
df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

unique_dates = sorted(df.date.unique())

old = unique_dates[0]
new = unique_dates[-1]
distance = new - old
distance = distance.astype('timedelta64[h]')
print(distance)


columns = ['mean', 'median', 'sum', 'count']

old_data = df[df.date == old].set_index('name')[columns]
new_data = df[df.date == new].set_index('name')[columns]


change = ((new_data - old_data) / new_data) * 100
change = new_data - old_data

_mean = change['mean'].sort_values().tail(20)
_median = change['median'].sort_values().tail(20)
_sum = change['sum'].sort_values().tail(20)

print(_mean)
print(_median)
print(_sum)

trace = go.Bar(x=_mean.index, y=_mean.values)
trace = go.Bar(x=_sum.index, y=_sum.values)
trace = go.Bar(x=_median.index, y=_median.values)
layout = go.Layout(title=f'Telegram User percent change [last {str(distance)}]'.upper())

fig = go.Figure(data=[trace], layout=layout)
py.plot(fig)
