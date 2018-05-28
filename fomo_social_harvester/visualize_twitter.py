from collections import Counter
from models.telegram import Telegram
from models.twitter import Twitter

import plotly
import plotly.offline as py
import plotly.graph_objs as go

import dash
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd
import numpy as np
#-----------


def get_twitter_fig():
    twitter_df = Twitter.query_all()
    twitter_df.reset_index(inplace=True)
    print(twitter_df.columns)
    print(twitter_df.head())
    print(twitter_df.info())

    df = twitter_df

    unique_dates = sorted(df.date.unique())
    old = unique_dates[0]
    new = unique_dates[-1]

    distance = new - old
    distance = distance.astype('timedelta64[h]')
    print(distance)
    print(df.head())

    columns = ['followers']
    old_data = df[df.date == old].set_index('name')[columns]
    new_data = df[df.date == new].set_index('name')[columns]

    change = new_data - old_data
    pct_change = ((change) / new_data) * 100

    top_change = change['followers'].sort_values().dropna().tail(20)
    top_pct_change = pct_change['followers'].sort_values().dropna().tail(20)

    # print(top_change)
    # print(top_pct_change)

    #
    top_trace_change = go.Bar(name='change',
                              x=top_change.index,
                              y=top_change.values)

    top_trace_pct_change = go.Bar(name='pct change',
                                  x=top_pct_change.index,
                                  y=top_pct_change.values)

    top_layout = go.Layout(title=f'Twitter User Top percent change [last {str(distance)}]'.upper(),
                           # barmode='stack',
                           yaxis=dict(type='log', autorange=True,
                                      showline=False, autotick=False,
                                      showticklabels=False))
    return go.Figure(data=[top_trace_pct_change, top_trace_change], layout=top_layout)
    # return go.Figure(data=[top_trace_pct_change], layout=top_layout)

#
# twitter_df = Twitter.query_all()
# twitter_df.reset_index(inplace=True)
# print(twitter_df.columns)
# print(twitter_df.head())
# print(twitter_df.info())
#
# df = twitter_df
#
# unique_dates = sorted(df.date.unique())
# old = unique_dates[0]
# new = unique_dates[-1]
#
# distance = new - old
# distance = distance.astype('timedelta64[h]')
# # df.groupby(['date', 'name'])['mean'].sum().groupby(level='name').mean()
# crypto_follower_change = df.groupby(['date'])['followers'].sum()
# crypto_follower_pct_change = crypto_follower_change.pct_change()
#
# overall_trace_change = go.Scatter(name='overall pct change',
#                                   x=crypto_follower_change.index,
#                                   y=crypto_follower_change.values)
#
# overall_trace_pct_change = go.Bar(name='overall pct change',
#                                   x=crypto_follower_pct_change.index,
#                                   y=crypto_follower_pct_change.values)
#
# top_layout = go.Layout(title=f'Twitter Overall Crypto User Growth percent change [last {str(distance)}]'.upper(),
#                        yaxis=dict(type='log', autorange=True,
#                                   showline=False, autotick=False,
#                                   showticklabels=False))
# #
# fig = go.Figure(data=[overall_trace_change], layout=top_layout)
# fig2 = go.Figure(data=[overall_trace_pct_change], layout=top_layout)
# app = dash.Dash()
# # #
# app.layout = html.Div(children=[
#     html.H1('Twitter-Member-Data'),
#     dcc.Graph(id='top_graph', figure=fig),
#     dcc.Graph(id='top_graph_2', figure=fig2),
#     #
# ])
# #
# app.run_server(debug=True)
