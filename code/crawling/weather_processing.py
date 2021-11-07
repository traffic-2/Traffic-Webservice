
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

# 실시간 날씨 json 데이터 받아오기
path = '/home/lab16/weather'
file_ = os.listdir(path)
file_json = [file for file in file_ if file.endswith('.json')]

with open('/home/lab16/weather/%s' % (file_json[0]), 'r') as f:
    j = json.load(f)
j_list = j['response']['body']['items']['item']

# j_list를 spark dataframe으로 변환
j_sdf = spark.createDataFrame(j_list)
j_sdf.createOrReplaceTempView('j_sdf')

# MySQL로부터 테이블 호출
realtime_weather = spark.read.format('jdbc').option('driver', 'com.mysql.jdbc.Driver').option('url', 'jdbc:mysql://{}:3306/{}'.format('3.37.159.174', 'finalPJT')).option('user', 'mulcam').option('password', 'mulcam').option('dbtable', 'realtime_weather').load()

# spark에서 SQL 쿼리를 위해 테이블 선언
realtime_weather.createOrReplaceTempView('realtime_weather')

# 데이터 삽입
spark.sql("INSERT INTO realtime_weather SELECT * FROM j_sdf")

# 폴더 내의 날씨 json 파일 삭제
[os.remove(f) for f in glob.glob("/home/lab16/weather/*.json")]

