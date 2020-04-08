import dash
import dash_core_components as dcc
import dash_html_components as html
from sshtunnel import SSHTunnelForwarder
import requests
from dash.dependencies import Input, Output
import plotly.graph_objs as go
# from apps.utils import options,options_won, figure_dict, figure_dict_won
# from apps.config import CODES_Y, KLINE_X
from pymongo import MongoClient
import pandas as pd
import dash_auth
#配置访问密码
VALID_USERNAME_PASSWORD_PAIRS = [
    ['ahrli', 'ahrli1'],
    ['test1', 'test1'],
    ['test3', 'ahrli3531'],

]
#风格组件
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
#密码认证
auth = dash_auth.BasicAuth(
    app,
    VALID_USERNAME_PASSWORD_PAIRS
)



options_time_longShortPositionRatio = [

           {'label': '多空比-5分钟', 'value': "5min"},
           {'label': '多空比-1小时', 'value': "1hour"},
           {'label': '多空比-1天', 'value': "1day"},
           ]
options_code = [
           {'label': 'BTC', 'value': "BTC"},
           {'label': 'ETH', 'value': "ETH"},
           {'label': 'EOS', 'value': "EOS"},
           {'label': 'LTC', 'value': "LTC"},
           {'label': 'XRP', 'value': "XRP"},
           {'label': 'ETC', 'value': "ETC"},
           {'label': 'BCH', 'value': "BCH"},
           {'label': 'BSV', 'value': "BSV"},
           {'label': 'TRX', 'value': "TRX"},
           ]






options_time_openInterestAndVolume = [

           {'label': '持仓量-5分钟', 'value': "5min"},
           {'label': '持仓量-1小时', 'value': "1hour"},
           {'label': '持仓量-1天', 'value': "1day"},
           ]

options_time_getFuturePositionRatio = [

           {'label': '精英持仓比-5分钟', 'value': "5min"},
           {'label': '精英持仓比-15分钟', 'value': "15min"},
           {'label': '精英持仓比-1小时', 'value': "1hour"},
           ]


app.layout = html.Div([
    dcc.Dropdown(
        options=options_time_longShortPositionRatio,
        value = "5min", # 默认值
        id = 'select_time'
    ),
    dcc.Dropdown(
        options=options_code,
        value = "BTC", # 默认值
        id = 'select_code'
    ),
    dcc.Graph(id='loop_longShortPositionRatio'),

    dcc.Dropdown(
        options=options_time_openInterestAndVolume,
        value = "5min", # 默认值
        id = 'select_time_openInterestAndVolume'
    ),
    dcc.Dropdown(
        options=options_code,
        value = "BTC", # 默认值
        id = 'select_code_openInterestAndVolume'
    ),
    dcc.Graph(id='loop_openInterestAndVolume'),

    dcc.Dropdown(
        options=options_time_getFuturePositionRatio,
        value = "5min", # 默认值
        id = 'select_time_getFuturePositionRatio'
    ),
    dcc.Dropdown(
        options=options_code,
        value = "BTC", # 默认值
        id = 'select_code_getFuturePositionRatio'
    ),
    dcc.Graph(id='loop_getFuturePositionRatio'),


])





def read_mongo(db='longShortPositionRatio',
               collection='1day',
               query={},
               select={},
               sortby="_id",
               upAndDown=1,  # 升序
               url='mongodb://root:dds-uf66@dds-uf66056e937ca0941.mongodb.rds.aliyuncs.com:3717,dds-uf66056e937ca0942.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-10450573',
               no_id=0,  # 默认不删除_id
               # _dtype='float16'
               ):
    """ Read from Mongo and Store into DataFrame """
    # Connect to MongoDB
    data = pd.DataFrame()

    server = SSHTunnelForwarder(
        ("39.108.65.107", 22),  # Remote server IP and SSH port
        ssh_username="root",
        ssh_password="Ahrli1pro1",
        remote_bind_address=("127.0.0.1", 27017))
    server.start()
    url = f'mongodb://localhost:{server.local_bind_port}/'
    # url = f'mongodb://localhost:27017/'
    with MongoClient(url) as client:
        collection = client[db][collection]

        data = collection.find(query)
        try:

            data = pd.DataFrame(list(data))
        except Exception:
            data = None
    server.close()
    return data






@app.callback(Output('loop_longShortPositionRatio', 'figure'),[Input('select_time', 'value'),Input('select_code', 'value'),])
def longShortPositionRatio(select_time,select_code):
    # 多空比
    # figure_dict = figure_dict
    ndf = read_mongo(collection=select_time,query = {'code':f'{select_code}'})
    ndf = ndf.sort_values('time_key' )
    # yield_result[2].reverse()
    trace = go.Scatter(
        x=list(ndf['time_key']),
        y=list(ndf['ratios']),
        name='比率',
        yaxis='y2'

    )
    trace_kline = go.Candlestick(x=list(ndf['time_key']),
                           open=ndf['open'],
                           high=ndf['high'],
                           low=ndf['low'],
                           close=ndf['close'],
                            hovertext = ndf['str_time'],
                           name='kline')



    return {
        'data': [trace_kline,trace],
        'layout': dict(
            title="多空比",

            height=800,
            yaxis2=dict(anchor='x', overlaying='y', side='right')  #
        )
    }



@app.callback(Output('loop_openInterestAndVolume', 'figure'),[Input('select_time_openInterestAndVolume', 'value'),Input('select_code_openInterestAndVolume', 'value'),])
def openInterestAndVolume(select_time,select_code):
    # 持仓量
    # figure_dict = figure_dict
    ndf = read_mongo(db='openInterestAndVolume' , collection=select_time,query = {'code':f'{select_code}'})
    ndf = ndf.sort_values('time_key')
    # yield_result[2].reverse()
    trace = go.Scatter(
        x=list(ndf['time_key']),
        y=list(ndf['openInterests']),
        name='总持仓量',
        yaxis='y2'

    )
    trace_bar = go.Bar(
        x=list(ndf['time_key']),
        y=[i/10000000  for i in list(ndf['volumes'])],
        name='交易量',
        yaxis='y3',
        marker=dict(
            color='rgb(158,222,225)',)

    )

    # trace_bar = go.Scatter(
    #     x=list(ndf['time_key']),
    #     y=[i / 10000000 for i in list(ndf['volumes'])],
    #     name='bbbccc',
    #     yaxis='y3'
    # )
    trace_kline = go.Candlestick(x=list(ndf['time_key']),
                           open=ndf['open'],
                           high=ndf['high'],
                           low=ndf['low'],
                           close=ndf['close'],
                            hovertext = ndf['str_time'],
                           name='kline')



    return {
        'data': [trace_kline,trace,trace_bar],
        'layout': dict(
            title="持仓量",

            height=800,
            yaxis2=dict(anchor='x', overlaying='y', side='right'),  #
            yaxis3=dict(anchor='x', overlaying='y', side='right')  #
        ),
        'shapes': [trace_bar]
    }





@app.callback(Output('loop_getFuturePositionRatio', 'figure'),[Input('select_time_getFuturePositionRatio', 'value'),Input('select_code_getFuturePositionRatio', 'value'),])
def getFuturePositionRatio(select_time,select_code):
    # 多空比
    # figure_dict = figure_dict
    ndf = read_mongo(db='getFuturePositionRatio' , collection=select_time,query = {'code':f'{select_code}'})
    ndf = ndf.sort_values('time_key')
    # yield_result[2].reverse()
    trace1 = go.Bar(
        x=list(ndf['time_key']),
        y=list(ndf['selldata']),
        marker=dict(
            color='rgb(158,202,225)',),

        name='空'
    )
    trace2 = go.Bar(
        x=list(ndf['time_key']),
        y=list(ndf['buydata']),
        # yaxis='y3',
        name='多',
        marker=dict(
            color='rgb(58,200,225)',)
    )

    trace_kline = go.Candlestick(x=list(ndf['time_key']),
                           open=ndf['open'],
                           high=ndf['high'],
                           low=ndf['low'],
                           close=ndf['close'],
                            hovertext = ndf['str_time'],
                                 yaxis='y2',
                           name='kline')



    return {
        'data': [trace1,trace2,trace_kline],
        'layout': dict(
            title="多空比",
            height=800,
            yaxis2=dict(anchor='x', overlaying='y', side='right'),  #
            # yaxis3=dict(anchor='x', overlaying='y', side='right')  #
        )
    }



if __name__ == '__main__':
    app.run_server(host="0.0.0.0")






