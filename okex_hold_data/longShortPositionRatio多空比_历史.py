#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time

import arrow
import requests
from dingtalkchatbot.chatbot import DingtalkChatbot
from pymongo import MongoClient
import pandas as pd

def rq(url,t,unitType,code,s,e):
    url = f"https://www.okex.me/v3/futures/pc/market/{url}/{code}"
    querystring = {"t":f'{t}',"unitType":f'{unitType}' ,
                   'startTime': s,
                    'endTime':e
                   }
    payload = ""
    headers = {
        'cache-control': "no-cache",
        'Postman-Token': "df324e89-9127-418b-bf81-7c4506e3561e"
        }
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)


    return response.json()

def save_mongo(db,collection,mylist,code):
    url =  'mongodb://root:dds-uf66@dds-uf66056e937ca0941.mongodb.rds.aliyuncs.com:3717,dds-uf66056e937ca0942.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-10450573'
    with MongoClient(url) as client:
        collection = client[db][collection]
        print(len(mylist))
        for i in mylist:
            condition = {'_id': code+'_'+str(i['time_key'])}
            collection.update(condition, i, upsert=True)
    pass

def cq(data):
    data = data['data']
    ratios = data['ratios']
    timestamps = data['timestamps']
    return dict(zip(timestamps,ratios))
def cq1(data):
    data = data['data']
    openInterests = data['openInterests']
    timestamps = data['timestamps']
    volumes = data['volumes']
    lt = []
    for i in zip(timestamps,volumes,openInterests):
        lt.append({'t':i[0],'volumes':i[1],'openInterests':i[2]})


    return lt




# @pysnooper.snoop()
def get_elite_df(coin="BTC", dim_size=60 * 5):
    """
    coin in (BTC, LTC, ETH, ETC, XRP, EOS, BCH, BSV, TRX)
    dim_size in (60 * 5, 60 * 15, 60 * 60)
    """
    # 统一 标签和时间间隔
    if dim_size == 60 * 5:
        dim = 0
    elif dim_size == 60 * 15:
        dim = 1
    elif dim_size == 60 * 60:
        dim = 2
    edf = get_elite_Position_Ratio_df(coin, dim)



    kdf = get_okex_kline(f"{coin}-USDT", dim_size)
    ndf = pd.merge(edf, kdf, left_index=True, right_index=True)
    del ndf["timestamps"]
    return ndf

def get_okex_kline(code="BTC-USDT", dim_size=900):

    url = f"https://www.okex.me/v2/spot/instruments/{code}/candles"
    querystring = {"granularity": dim_size, "size": "1000"}
    payload = ""
    headers = {
        'accept':
        "application/json",
        'accept-encoding':
        "gzip, deflate, br",
        'accept-language':
        "zh-CN,zh;q=0.9",
        'app-type':
        "web",
        'cookie':
        "__cfduid=dccb6eb64863762d5419010f21162882c1572510668; locale=zh_CN; first_ref=https%3A%2F%2Fwww.google.com%2F; _bl_uid=h5k102tFe5vgXX9mp3as81dbsO1y",
        'devid':
        "fa6e20fb-17c8-4f5c-a6d8-2695d09dfbc8",
        'referer':
        "https://www.okex.com/spot/full",
        'sec-fetch-mode':
        "cors",
        'sec-fetch-site':
        "same-origin",
        'timeout':
        "10000",
        'user-agent':
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36"
    }
    response = requests.request("GET",
                                url,
                                data=payload,
                                headers=headers,
                                params=querystring)
    if response.ok == 1 and response.json()['code'] == 0:
        df = pd.DataFrame(response.json()['data'])
        df.columns = ['time_date_0', 'open', 'high', 'low', 'close', 'volume']
        df['time_date'] = conduct_dataframe(
            df, 'time_date_0', lambda _date: arrow.get(_date).to("local").format(
                "YYYY-MM-DD HH:mm:ss ZZ"))
        df['time_key'] = conduct_dataframe(
            df, 'time_date_0',
            lambda _date: arrow.get(_date).to("local").timestamp)
        del df['time_date_0']
        df.set_index(
            ['time_date'],
            inplace=True,
        )
        for i in df.columns:
            if i == "time_key":
                df[f"{i}"] = conduct_dataframe(df,i,int)
            else:
                df[f"{i}"] = conduct_dataframe(df,i,float)
#         df = df.loc[df['time_key'] <= 1574764740]
        return df

def conduct_dataframe(df,conductField,func,*arg):
    iterator = map(func,df[conductField],*arg)
    iteratorS = pd.Series(iterator,index=df.index)
    return iteratorS

def get_elite_Position_Ratio_df(coin="BTC",dim=0):
    url = f"https://okexcomweb.bafang.com/v3/futures/app/market/elitePositionRatio/{coin}"
    querystring = {"unitType": f"{dim}"}

    payload = ""
    headers = {
        'cookie': "locale=zh_CN",
        'host': "okexcomweb.bafang.com",
        'accept': "*/*",
        'authorization':
        "eyJhbGciOiJIUzUxMiJ9.eyJqdGkiOiJleDEyMDE1NzQ4NTA5OTc0NzIyQzQyRTRGRjA0ODNGODYxWnh2dCIsInVpZCI6IjQyR1JMaXJVRlhUMWhUOTlsM1ZrWXc9PSIsInN0YSI6MCwibWlkIjowLCJpYXQiOjE1NzU0NDExNTgsImV4cCI6MTU3NjA0NTk1OCwiYmlkIjowLCJkb20iOiJva2V4Y29td2ViLmJhZmFuZy5jb20iLCJpc3MiOiJva2NvaW4iLCJzdWIiOiJEOEM3QTY4RTlEOEEzNTFEMjRERjVBMDIzNTQxOUMwQSJ9.yggwstQYcAppU-pb-k0Uz6ofgaGvn6yBtYYcsIyEnz-sOrv8x3sOKLIALxWX9h_S2Hhf-dPTWXmz2gk0zzd0wg",
        'bundleid': "com.okex.OKExAppstoreFull",
        'cache-control': "no-cache",
        'devid': "0BD85D71-841D-4649-92D4-E8841D3F8C17",
        'accept-language': "zh-Hans",
        'content-type': "application/json",
        'referer':
        "https://okexcomweb.bafang.com/v3/futures/app/market/elitePositionRatio/BTC",
        'user-agent':
        "OKEx/3.3.2 (iPhone;U;iOS 13.1.2;zh-CN/zh-Hans) locale=zh-Hans"
    }

    response = requests.request("GET",
                                url,
                                data=payload,
                                headers=headers,
                                params=querystring)

    df = pd.DataFrame(response.json()["data"])
    for i in df.columns:
        if i == "timestamps":
            df[f"{i}"] = conduct_dataframe(df,i,lambda _str:arrow.get(int(float(_str)/1000)).floor('minute'))
        else:
            df[f"{i}"] = conduct_dataframe(df,i,float)
    df['time_date'] = conduct_dataframe(
        df, 'timestamps',
        lambda ts: arrow.get(ts).to('local').format("YYYY-MM-DD HH:mm:ss ZZ"))
    df.set_index(
        ['time_date'],
        inplace=True,
    )
    df['coin'] = coin
    return df





def run(ltttt):
    'longShortPositionRatio'
    for code in [
        'BTC',
                 'ETH',
                 'LTC','ETC','XRP','EOS','BCH','BSV','TRX'
                 ]:
        for url in [
            'openInterestAndVolume',
            'longShortPositionRatio'
        ]:
            t = arrow.now().timestamp
            for i in [
                (0,'5min'),
                # (1,'1hour'),
                # (2,'1day')
            ]:
                if i[0] == 0:
                    dim_size = 60 * 5
                elif i[0] == 1:
                    dim_size = 60 * 60
                elif i[0] == 2:
                    dim_size = 60 * 60 * 24

                kline_data = get_okex_kline(code=f"{code}-USDT", dim_size=dim_size)
                kline_data['str_time'] = kline_data.index
                kline_data = kline_data.rename({'volume': 'real_volumes'}, axis=1)
                for ti in ltttt:
                    print(ti,code)
                    s,t=ti[0],ti[1]
                    data = rq(url,t, i[0],code,s,t)
                    data_df = pd.DataFrame(data['data'])
                    data_df['timestamps'] = data_df['timestamps'].apply(lambda  x : int(x/1000))
                    data_df = data_df.set_index('timestamps')


                    df = pd.merge(data_df, kline_data, left_index=True, right_on='time_key')
                    df['code'] = code
                    mylist = df.to_dict('recordes')
                    save_mongo(url,i[1],mylist,code)



lt = []
for i in range(6):
    s = arrow.now().shift(days=-1*i,hours=-12).format(fmt='YYYY-MM-DD HH:mm:ss')
    e = arrow.get(s).shift(days=1).format(fmt='YYYY-MM-DD HH:mm:ss')
    lt.append((s,e))
print(lt[3])
print(lt[-1])
# run([('2019-12-05 00:00:00', '2019-12-09 00:00:00')])
run(lt)




#
# while True:
#     try:
#         run()
#         time.sleep(60*10)
#     except Exception:
#         webhook = 'https://oapi.dingtalk.com/robot/send?access_token=5b6f6510bcbaa1f2189d0e3824b4ba2f8a8c19f406f30e672044688c3d323f5b'
#         # 初始化机器人小丁
#         xiaoding = DingtalkChatbot(webhook)
#         a = '程序\n' + '精英持仓比程序错误'
#         xiaoding.send_text(msg=a, is_at_all=False)
#         time.sleep(60 * 10)





