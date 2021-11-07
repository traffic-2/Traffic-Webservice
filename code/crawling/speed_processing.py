
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
path = '/home/lab16/vol'
file_ = os.listdir(path)
file_json = [file for file in file_ if file.endswith('.json')]


# 교통량측정지점, 사고다발지점 컬럼 추가
accident_list = ['안국역사거리', '광하문우체국사거리','종로3가역사거리', '이화사거리', '동묘앞역사거리']
traffic_list = ['율곡로(안국역)', '세종대로(시청역2)', '종로(종로3가역)', '대학로(한국방송통신대학교)', '종로(동묘앞역)']
speed_list = ['1000011600', '1000008300', '1000001100', '1000009000', '1000002200']

data = {}
data2 = {}
# 링크아이디 통해 교통량측정지점 위치 확인
for i in range(len(speed_list)):
    data[speed_list[i]] = traffic_list[i]
    
# 링크아이디 통해 사고다발지점 위치 확인
for i in range(len(speed_list)):
    data2[speed_list[i]] = accident_list[i]
    
    
# json 데이터 처리    
processing_list = []

for i in range(len(file_json)):
    with open('/home/lab16/vol/%s' % (file_json[i]), 'r') as f:
        j = json.load(f)
        j_list = j['TrafficInfo']['row']
        #print(j_list)
        
        # 파일명으로부터 데이터 추출
        year = file_json[i][:4]
        month = file_json[i][4:6]
        day = file_json[i][6:8]
        hour = file_json[i][8:10]
        
        d = dict()
        
        d['year'] = year
        d['month'] = month
        d['day'] = day
        d['hour'] = hour
        d['link_id'] = j_list['link_id']
        d['spot_name'] = data[j_list['link_id']]
        d['spot_name2'] = data2[j_list['link_id']]
        d['speed'] = j_list['prcs_spd']
        
        processing_list.append(d)
        
        
# processing_list를 spark dataframe으로 변환
j_sdf = spark.createDataFrame(processing_list)
j_sdf = j_sdf.select(j_sdf.year, j_sdf.month, j_sdf.day, j_sdf.hour, j_sdf.link_id, j_sdf.spot_name, j_sdf.spot_name2, j_sdf.speed)

j_sdf.createOrReplaceTempView('j_sdf')


# MySQL로부터 테이블 호출
realtime_speed = spark.read.format('jdbc').option('driver', 'com.mysql.jdbc.Driver').option('url', 'jdbc:mysql://{}:3306/{}'.format('3.37.159.174', 'finalPJT')).option('user', 'mulcam').option('password', 'mulcam').option('dbtable', 'realtime_speed').load()

# spark에서 SQL 쿼리를 위해 테이블 선언
realtime_speed.createOrReplaceTempView('realtime_speed')


# 데이터 삽입
spark.sql("INSERT INTO realtime_speed SELECT * FROM j_sdf")


# 폴더 내의 날씨 json 파일 삭제
[os.remove(f) for f in glob.glob("/home/lab16/vol/*.json")]

