from collections import Counter
from models.telegram import Telegram
from visualize_twitter import get_twitter_fig
import plotly
import plotly.offline as py
import plotly.graph_objs as go

import dash
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd
import numpy as np
#-----------


def create_top_telegram_graph(dataframe, sample_hours_=None):
    sample_hours = sample_hours_
    df = dataframe

    numeric_columns = ['mean', 'median', 'sum']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    unique_dates = sorted(df.date.unique())

    if sample_hours:
        old = unique_dates[-(sample_hours + 1)]
    else:
        old = unique_dates[0]

# df.groupby(['date', 'name'])['mean'].sum().groupby(level='name').mean()
    new = unique_dates[-1]

    distance = new - old
    distance = distance.astype('timedelta64[h]')
    print(distance)
    #
    #
    columns = ['mean', 'median', 'sum', 'count']
    #
    old_data = df[df.date == old].set_index('name')[columns]
    new_data = df[df.date == new].set_index('name')[columns]
    #
    #
    change_sum = new_data - old_data  # IDEA change_n experiemtn
    change = ((new_data - old_data) / new_data) * 100
    #
    _mean = change['mean'].sort_values().tail(20)
    _median = change['median'].sort_values().tail(20)
    _sum = change['sum'].sort_values().tail(20)  # IDEA change_n experiemtn
    #
    # print(_mean)
    # print(_median)
    # print(_sum)
    top_names = list(_mean.index) + list(_median.index) + list(_sum.index)
    top_names = Counter(top_names)
    print(top_names)
    top_names = [name for name in top_names if top_names[name] > 1]

    print(change.ix[top_names][['mean', 'median', 'sum']])

    top_change = change.ix[top_names][['mean', 'median', 'sum']]
    top_change_sum = change_sum.ix[top_names][['mean', 'median', 'sum']].sort_values(by='sum')

    top_trace_mean = go.Bar(name='mean', x=top_change_sum.index, y=top_change['mean'])
    top_trace_median = go.Bar(name='median', x=top_change_sum.index, y=top_change['median'])
    top_trace_sum = go.Bar(name='sum', x=top_change_sum.index, y=top_change_sum['sum'])
    top_layout = go.Layout(title=f'Telegram User Top percent change [last {str(distance)}]'.upper(),
                           barmode='stack',
                           yaxis=dict(type='log', autorange=True,
                                      showline=False, autotick=False,
                                      showticklabels=False))
    return go.Figure(data=[top_trace_mean, top_trace_median, top_trace_sum], layout=top_layout)


#----------------dash
telegram_df = Telegram.query_all()
telegram_df.reset_index(inplace=True)

top_telegram = create_top_telegram_graph(telegram_df, sample_hours_=None)
top_twitter = get_twitter_fig()
# top_twelve = create_top_telegram_graph(telegram_df, sample_hours_=12)

app = dash.Dash()

app.layout = html.Div(children=[
    html.H1('Telegram-Member-Data'),
    dcc.Graph(id='top_telegram', figure=top_telegram),
    dcc.Graph(id='top_twitter', figure=top_twitter),

])

app.run_server(debug=True)
