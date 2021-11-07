#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import glob
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.conf import SparkConf

conf = SparkConf().setAppName('spark-sql').set('spark.driver.extraClassPath', '/home/lab16/Final_PJT/data/mysql-connector-java-8.0.26.jar')

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession

# 실시간 교통량 json 데이터 받아오기
path = '/home/lab16/traffic'
file_ = os.listdir(path)
file_json = [file for file in file_ if file.endswith('.json')]


# json 데이터 처리
processing_list = []

for i in range(len(file_json)):
    with open('/home/lab16/traffic/%s' % (file_json[i]), 'r') as f:
        j = json.load(f)
        j_list = j['VolInfo']['row']

    # 하나의 지점 - 하나의 교통량 형태로 만들기
    sum = 0
    for i in range(len(j_list)):
        sum += int(j_list[i]['vol'])
        #print(j_list[i])
    sum //= 2

    d = dict()
    d['spot_num'] = j_list[0]['spot_num']
    d['ymd'] = j_list[0]['ymd']
    d['hh'] = j_list[0]['hh']
    d['vol'] = sum
    
    if j_list[0]['spot_num'] == 'A-08':
        d['spot_name'] = '종로(동묘앞역)'
    elif j_list[0]['spot_num'] == 'A-07':
        d['spot_name'] = '대학로(한국방송통신대학교)'
    elif j_list[0]['spot_num'] == 'A-15':
        d['spot_name'] = '종로(종로3가역)'
    elif j_list[0]['spot_num'] == 'A-17':
        d['spot_name'] = '세종대로(시청역2)'
    else:
        d['spot_name'] = '율곡로(안국역)'
        
    processing_list.append(d)
    

# processing_list를 spark dataframe으로 변환
j_sdf = spark.createDataFrame(processing_list)
j_sdf = j_sdf.select(j_sdf.ymd, j_sdf.hh, j_sdf.spot_num, j_sdf.vol, j_sdf.spot_name)

j_sdf.createOrReplaceTempView('j_sdf')


# MySQL로부터 테이블 호출
realtime_traffic = spark.read.format('jdbc').option('driver', 'com.mysql.jdbc.Driver').option('url', 'jdbc:mysql://{}:3306/{}'.format('3.37.159.174', 'finalPJT')).option('user', 'mulcam').option('password', 'mulcam').option('dbtable', 'realtime_traffic').load()

                                                    
# spark에서 SQL 쿼리를 위해 테이블 선언
realtime_traffic.createOrReplaceTempView('realtime_traffic')


# 데이터 삽입
spark.sql("INSERT INTO realtime_traffic SELECT * FROM j_sdf")


# 폴더 내의 날씨 json 파일 삭제
[os.remove(f) for f in glob.glob("/home/lab16/traffic/*.json")]

