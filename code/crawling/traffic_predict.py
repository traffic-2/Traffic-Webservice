#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime
import pandas as pd
import numpy as np

from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.types import *

#################### 교통량 데이터 업데이트 #########################

df_before = pd.read_csv('/home/lab16/Final_PJT/2020~20210930_교통량데이터.csv', encoding='utf-8')

# MySQL로부터 테이블 호출
conf = SparkConf().setAppName('spark-sql').set('spark.driver.extraClassPath', '/home/lab16/Final_PJT/data/mysql-connector-java-8.0.26.jar')

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession

realtime_traffic = spark.read.format('jdbc').option('driver', 'com.mysql.jdbc.Driver').option('url', 'jdbc:mysql://{}:3306/{}'.format('3.37.159.174', 'finalPJT')).option('user', 'mulcam').option('password', 'mulcam').option('dbtable', 'realtime_traffic').load()

# spark에서 SQL 쿼리를 위해 테이블 선언
realtime_traffic.createOrReplaceTempView('realtime_traffic')

realtime_traffic = realtime_traffic.sort(['baseDate','baseTime'])

d = spark.sql("SELECT baseDate, baseTime, vol, spot_name FROM realtime_traffic ORDER BY baseDate, baseTime")
d = d.select("*").toPandas()
d = d.astype({'vol':'float'})

# 교통량 빈값 평균값으로 처리
for idx, val in d.iterrows():
    if val['vol'] == 0:
        d__ = d.groupby(['spot_name']).mean()
        d['vol'][idx] = np.round_(d__['vol'][0])
        
# 과거 데이터 + 최신 데이터 합치기 위해 MySQL DB에 저장된 데이터 형태를 변경
d['년도'] = ''
d['월'] = ''
d['일'] = ''

for idx, val in d.iterrows():
    d['년도'][idx] = d['baseDate'][idx][:4]
    d['월'][idx] = d['baseDate'][idx][4:6]
    d['일'][idx] = d['baseDate'][idx][6:]
    
d['ds'] = d['년도'].apply(str)+'-' + d['월'].apply(str)+'-'  + d['일'].apply(str)+'-' + d['baseTime'].apply(str)
d['ds'] = pd.to_datetime(d['ds'],format='%Y-%m-%d-%H')

accident_list = ['안국역사거리', '광하문우체국사거리','종로3가역사거리', '이화사거리', '동묘앞역사거리']
traffic_list = ['율곡로(안국역)', '세종대로(시청역2)', '종로(종로3가역)', '대학로(한국방송통신대학교)', '종로(동묘앞역)']

data = {}
for i in range(len(accident_list)):
    data[traffic_list[i]] = accident_list[i]

d['사고다발지점'] = ''
for idx, val in d.iterrows():
    d['사고다발지점'][idx] = data[val['spot_name']]
    
d['요일'] = np.nan
d = d.rename(columns={'vol':'y', 'baseTime':'시간', 'spot_name':'교통량측정지점명'})
d = d[['년도','월','일','요일','시간','교통량측정지점명','사고다발지점','y','ds']]

d = pd.concat([d.query('ds.dt.dayofweek == 0').fillna('월'), d.query('ds.dt.dayofweek == 1').fillna('화'), d.query('ds.dt.dayofweek == 2').fillna('수'), d.query('ds.dt.dayofweek == 3').fillna('목'), d.query('ds.dt.dayofweek == 4').fillna('금'), d.query('ds.dt.dayofweek == 5').fillna('토'), d.query('ds.dt.dayofweek == 6').fillna('일')])
d = d.sort_values(['년도','월','일','시간'])

# 과거데이터 + 최신데이터 결합
traffic_df = pd.concat([df_before, d])
traffic_df = traffic_df.astype(
                        {'년도':int,
                        '월':int,
                        '일':int,
                        '시간':int})
traffic_df['ds'] = pd.to_datetime(traffic_df['ds'])



#######################################################################




df_1 = traffic_df[traffic_df['사고다발지점']=='이화사거리']
df_2 = traffic_df[traffic_df['사고다발지점']=='광하문우체국사거리']
df_3 = traffic_df[traffic_df['사고다발지점']=='안국역사거리']
df_4 = traffic_df[traffic_df['사고다발지점']=='동묘앞역사거리']
df_5 = traffic_df[traffic_df['사고다발지점']=='종로3가역사거리']




############################ 모델 재학습 ##############################

from fbprophet import Prophet
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import warnings
warnings.filterwarnings("ignore")

import pickle

########### 1. 이화사거리 ###########
df_1['cap'] = 2800
df_1['floor'] = 220

day = 1     # 테스트할 일수
x = 90 + day   # 학습할 일수

# 학습/예측에 사용할 데이터
train_df = df_1.iloc[-x*24:-day*24]

# 실제값으로 사용 : 예측한 값과 비교할 값
test_df = df_1.iloc[-day*24:]

# 모델 생성
prophet = Prophet(seasonality_mode='multiplicative',
                 yearly_seasonality=True, 
                 weekly_seasonality=True,
                 daily_seasonality=True,
                 changepoint_prior_scale=0.5)
model_fit = prophet.fit(train_df)

pickle.dump(model_fit, open(r'/home/lab07/final_tra/finalPJT/website/dashboard/df1.pkl', 'wb'))

########### 2. 광하문우체국사거리 ###########
df_2['cap'] = 2800
df_2['floor'] = 220

day = 1     # 테스트할 일수
x = 90 + day   # 학습할 일수
  
# 학습/예측에 사용할 데이터
train_df = df_2.iloc[-x*24:-day*24]

# 실제값으로 사용 : 예측한 값과 비교할 값
test_df = df_2.iloc[-day*24:]

# 모델 생성
prophet = Prophet(seasonality_mode='multiplicative',
                 yearly_seasonality=True, 
                 weekly_seasonality=True,
                 daily_seasonality=True,
                 changepoint_prior_scale=0.5)
model_fit = prophet.fit(train_df)

pickle.dump(model_fit, open(r'/home/lab07/final_tra/finalPJT/website/dashboard/df2.pkl', 'wb'))

########### 3. 안국역사거리 ###########
df_3['cap'] = 2800
df_3['floor'] = 220

day = 1     # 테스트할 일수
# x = 90 + day   # 학습할 일수
  
# 학습/예측에 사용할 데이터
train_df = df_3.iloc[-x*24:-day*24]

# 실제값으로 사용 : 예측한 값과 비교할 값
test_df = df_3.iloc[-day*24:]

# 모델 생성
prophet = Prophet(seasonality_mode='multiplicative',
                 yearly_seasonality=True, 
                 weekly_seasonality=True,
                 daily_seasonality=True,
                 changepoint_prior_scale=0.5)
model_fit = prophet.fit(train_df)

pickle.dump(model_fit, open(r'/home/lab07/final_tra/finalPJT/website/dashboard/df3.pkl', 'wb'))

########### 4. 동묘앞역사거리 ###########
df_4['cap'] = 2800
df_4['floor'] = 220

day = 1     # 테스트할 일수
# x = 90 + day   # 학습할 일수
  
# 학습/예측에 사용할 데이터
train_df = df_4.iloc[-x*24:-day*24]

# 실제값으로 사용 : 예측한 값과 비교할 값
test_df = df_4.iloc[-day*24:]

# 모델 생성
prophet = Prophet(seasonality_mode='multiplicative',
                 yearly_seasonality=True, 
                 weekly_seasonality=True,
                 daily_seasonality=True,
                 changepoint_prior_scale=0.5)
model_fit = prophet.fit(train_df)

pickle.dump(model_fit, open(r'/home/lab07/final_tra/finalPJT/website/dashboard/df4.pkl', 'wb'))

########### 5. 종로3가역사거리 ###########
df_5['cap'] = 2800
df_5['floor'] = 220

day = 1     # 테스트할 일수
# x = 90 + day   # 학습할 일수
  
# 학습/예측에 사용할 데이터
train_df = df_5.iloc[-x*24:-day*24]

# 실제값으로 사용 : 예측한 값과 비교할 값
test_df = df_5.iloc[-day*24:]

# 모델 생성
prophet = Prophet(seasonality_mode='multiplicative',
                 yearly_seasonality=True, 
                 weekly_seasonality=True,
                 daily_seasonality=True,
                 changepoint_prior_scale=0.5)
model_fit = prophet.fit(train_df)

pickle.dump(model_fit, open(r'/home/lab07/final_tra/finalPJT/website/dashboard/df5.pkl', 'wb'))


# In[ ]:




