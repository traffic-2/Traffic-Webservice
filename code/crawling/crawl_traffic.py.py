#!/usr/bin/env python
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






# table4 = pd.DataFrame(columns=['spot_num', 'ymd', 'hh', 'io_type', 'lane_num', 'vol'])
list_2=['A-05','A-17','A-15','A-07','A-08']

s = requests.Session()

retries = Retry(total=5,
               backoff_factor=4, # 2, 4, 8, 16, 32
               status_forcelist=[500, 502, 503, 504])

headers= {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0'
}







# def crawl(base_url, params, directory, name):
    
def crawl(url, directory, name):
    today = datetime.datetime.now()
    
#         res = s.get(base_url, headers=headers, params=params)

    res = s.get(url, headers=headers)
    
    print(res.status_code)
    
    if res.status_code == 200:
        
        
        print(res.content)
        
        result = json.loads(json.dumps(xmltodict.parse(res.content.decode('utf-8')), ensure_ascii=False))
        with open(directory + today.strftime("%Y%m%d%H%M%S") + name + ".json", "w", encoding='utf-8') as json_file:
     
            print(result)
            json.dump(result, json_file, ensure_ascii=False)
        
        return result
    else:
        return res.status_code    
    
    
    
    
    
    
keys = '--------------'

    
today = datetime.datetime.now()   

for i in list_2:
    
    
    url = 'http://openapi.seoul.go.kr:8088/'
    queryParams = keys+'/xml'+'/VolInfo'+'/1'+'/100/'+i+'/20210928/'+(today - datetime.timedelta(hours=1)).strftime("%H")
    
    url+=queryParams
    response= requests.get(url)
    
    crawl( url, './traffic/', 'traffic_'+ i)
    
    
    
    
    
    
#     if response2.status_code==200:
#         xd2 = xmltodict.parse(response2.text)
#         xdj2 = json.dumps(xd2, ensure_ascii = False)
#         xdjd2 = json.loads(xdj2)
        
#     table3=pd.json_normalize(xdjd2['VolInfo']['row'])
        
#     table4 = table4.append(pd.DataFrame(table3, columns=['spot_num', 'ymd', 'hh', 'io_type', 'lane_num', 'vol']), ignore_index=True)
    
    

    
# to mysql


# In[ ]:




