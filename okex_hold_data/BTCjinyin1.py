#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time

import arrow
import requests
from dingtalkchatbot.chatbot import DingtalkChatbot
from pymongo import MongoClient
import pandas as pd



def save_mongo(db,collection,mylist):

    url =  'mongodb://root:dds-uf66@dds-uf66056e937ca0941.mongodb.rds.aliyuncs.com:3717,dds-uf66056e937ca0942.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-10450573'

    with MongoClient(url) as client:
        collection = client[db][collection]
        print(len(mylist))
        for i in mylist:
            condition = {'_id': i['code']+'_'+str(i['time_key'])}
            collection.update(condition, i, upsert=True)
    pass


def cq1(data):
    data = data['data']
    buydata = data['buydata']
    timedata = data['timedata']
    selldata = data['selldata']
    lt = []
    for i in zip(timedata,selldata,buydata):
        lt.append({'t':i[0],'volumes':i[1],'openInterests':i[2]})


    return lt






def rq1(t,unitType,code):
    code = code.lower()
    url = "https://www.okex.me/v2/futures/pc/public/getFuturePositionRatio.do"
    querystring = {"t":f'{t}',"type":f'{unitType}',"symbol":f"f_usd_{code}",
                   # 'startTime': '2019-11-05 12:00:00',
                   # 'endTime': '2019-12-05 17:00:00'

                   }
    payload = ""
    headers = {
        'cache-control': "no-cache",
        'Postman-Token': "df324e89-9127-418b-bf81-7c4506e3561e"
        }
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)


    return response.json()
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

def run():
    t = arrow.now().timestamp
    for code in ['BTC','ETH',  'LTC', 'ETC', 'XRP', 'EOS', 'BCH', 'BSV', 'TRX']:
        print(code)
        for i in [(0, '5min'), (1, '15min'), (2, '1hour')]:
            if i[0] == 0:
                dim_size = 60 * 5
            elif i[0] == 1:
                dim_size = 60 * 15
            elif i[0] == 2:
                dim_size = 60 * 60

            data = rq1( t, i[0],code)
            data_df = pd.DataFrame(data['data'])
            data_df = data_df.rename({'timedata': 'timestamps'}, axis=1)
            data_df['timestamps'] = data_df['timestamps'].apply(lambda x: int(int(x.replace('1000','0000')) / 1000))
            data_df['timestamps'] = data_df['timestamps'].apply(lambda x:( arrow.get(x).shift(hours=0).to("local").timestamp))
            # data_df['time_date_str'] = conduct_dataframe( data_df, 'timestamps', lambda _date: arrow.get(_date).to("local").format( "YYYY-MM-DD HH:mm:ss ZZ"))
            data_df = data_df.set_index('timestamps')
            kline_data = get_okex_kline(code=f"{code}-USDT", dim_size=dim_size)
            kline_data['str_time'] = kline_data.index
            kline_data = kline_data.rename({'volume': 'real_volumes'}, axis=1)
            df = pd.merge(data_df, kline_data, left_index=True, right_on='time_key')
            df['code'] = code
            mylist = df.to_dict('recordes')
            save_mongo('getFuturePositionRatio', i[1], mylist)




# run()

while True:
    try:
        run()
        time.sleep(60*10)
        # raise 'ad'
    except Exception:
        webhook = 'https://oapi.dingtalk.com/robot/send?access_token=5b6f6510bcbaa1f2189d0e3824b4ba2f8a8c19f406f30e672044688c3d323f5b'
        # 初始化机器人小丁
        xiaoding = DingtalkChatbot(webhook)
        a = '程序\n' + '精英持仓比程序错误'
        xiaoding.send_text(msg=a, is_at_all=False)
        time.sleep(60 * 10)

# from apscheduler.schedulers.blocking import BlockingScheduler
# sched = BlockingScheduler(timezone="Asia/Shanghai")
# sched.add_job(run, 'cron', day = '*',hour = '*',minute = '*/10',second = 0)
# sched.add_job(ceshi, 'cron', day = '*',hour = '*/2',minute = 12,second = 0)
# sched.add_job(ceshi, 'cron', day = '*',hour = '*/1',minute = 30,second = 0)
# sched.start()








