#!/usr/bin/env python3
# coding: utf-8

# In[ ]:


# import pymysql
import requests
# from bs4 import BeautifulSoup
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import time
import os
import json
import datetime
import xmltodict

print('hi')

s = requests.Session()
retries = Retry(total=5,
               backoff_factor=4, # 2, 4, 8, 16, 32
               status_forcelist=[500, 502, 503, 504])
headers= {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0'
}

def crawl(base_url, params, directory, name):
    today = datetime.datetime.now()
    res = s.get(base_url, headers=headers, params=params)
    if res.status_code == 200:
        result = json.loads(json.dumps(xmltodict.parse(res.content.decode('utf-8')), ensure_ascii=False))
        with open(directory + today.strftime("%Y%m%d%H%M%S") + name + ".json", "w", encoding='utf-8') as json_file:
            json.dump(result, json_file, ensure_ascii=False)
        return result
    else:
        return res.status_code

keys = [
    '-------------------------',
]

# 1시간 간격 or 하루 마지막에 한번만
today = datetime.datetime.now()

base_url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst'
params = {
    'ServiceKey': keys,
    'pageNo': 1,
    'numOfRows': 100,
    'dataType': 'XML',
    'base_date': today.strftime("%Y%m%d"),
    'base_time': (today - datetime.timedelta(hours=1)).strftime("%H00"),
    'nx': 60,
    'ny': 127,
     }

crawl(base_url, params, './weather/', 'Weather'+'x')

