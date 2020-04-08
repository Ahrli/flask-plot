#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time

import arrow
import requests
from dingtalkchatbot.chatbot import DingtalkChatbot
from pymongo import MongoClient


def rq(url,t,unitType):
    url = f"https://www.okex.me/v3/futures/pc/market/{url}/BTC"
    querystring = {"t":f'{t}',"unitType":f'{unitType}'}
    payload = ""
    headers = {
        'cache-control': "no-cache",
        'Postman-Token': "df324e89-9127-418b-bf81-7c4506e3561e"
        }
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)


    return response.json()

def save_mongo(db,collection,mylist):
    url =  'mongodb://root:dds-uf66@dds-uf66056e937ca0941.mongodb.rds.aliyuncs.com:3717,dds-uf66056e937ca0942.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-10450573'
    with MongoClient(url) as client:
        collection = client[db][collection]
        print(len(mylist))
        for i in mylist:
            condition = {'_id': i['t']}
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

def run():
    'longShortPositionRatio'
    for url in ['openInterestAndVolume','longShortPositionRatio']:
        t = arrow.now().timestamp
        for i in [(0,'5min'),(1,'1hour'),(2,'1day')]:
            data = rq(url,t, i[0])
            if url =='longShortPositionRatio':
                data = cq(data,)
                data = [ {'t':k,'v':v} for k,v in data.items()]
            elif  url =='openInterestAndVolume'  :

                data = cq1(data)
            save_mongo(url,i[1],data)


# run()


while True:
    try:
        run()
        time.sleep(60*10)
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







