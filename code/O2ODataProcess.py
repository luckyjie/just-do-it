# -*- coding:utf-8 -*-
'''
created date:20161012
author:wangjin
hadoop  fs  -copyFromLocal /data/home/hadoop/wangjin/lifeO2O/ccf_offline_test.txt /hadoop/hadoop_/wangjin
hadoop fs -rm  /hadoop/hadoop_/wangjin/ccf_offline_train.csv 

/hadoop/hadoop_/wangjin/ccf_online_train

onlinetrain:
[u'13740231,18907,2,100017492,500:50,20160513,null']

offlinetrain:
[u'1439408,2632,null,null,0,null,20160217']

offlinetest
[u'4129537,450,9983,30:5,1,20160712']
'''
from pyspark.sql import SQLContext,Row
def process(line):
    line=line.split(':')
    if(len(line)>=2):
        return line
    else:
        line.append('0')
        return line
def processed(sc):
    #online_train
    data=sc.textFile('/hadoop/hadoop_/wangjin/ccf_online_train.txt')
    sqlC = SQLContext(sc)
    data=data.map(lambda x:x.split(','))
    dataTrain=data.map(lambda x:(x[0],x[1],x[2],x[3],process(x[4])[0],process(x[4])[1],x[5],x[6]))
    dataOnlineName=Row('user_id','Merchant_id','Action','Coupon_id','Discount_rate0','Discount_rate1','Date_received','Date')
    dataOnline=dataTrain.map(lambda x:dataOnlineName(*x))
    dataOnline1=sqlC.createDataFrame(dataOnline)
    sqlC.registerDataFrameAsTable(dataOnline1,"dataOnline")
    dataOnline2=sqlC.sql("select user_id,Merchant_id,Action,Coupon_id,Discount_rate0,Discount_rate1,Date_received,Date from dataOnline") 
    dataOnline2.repartition(5).write.save('/hadoop/hadoop_/wangjin/ccf_online_train.parquet')
    #dataOnline=dataTrain.repartition(5)
    #dataOnline.saveAsPickleFile('/hadoop/hadoop_/wangjin/ccf_online_train')
    
    #offline_train
    dataOff=sc.textFile('/hadoop/hadoop_/wangjin/ccf_offline_train.txt')
    dataOff=dataOff.map(lambda x:x.split(','))
    dataOffline=dataOff.map(lambda x:(x[0],x[1],x[2],process(x[3])[0],process(x[3])[1],x[4],x[5],x[6]))
    dataOfflineName=Row('user_id','Merchant_id','Coupon_id','Discount_rate0','Discount_rate1','Distance','Date_received','Date')
    dataOffline=dataOffline.map(lambda x:dataOfflineName(*x))
    dataOffline1=sqlC.createDataFrame(dataOffline)
    sqlC.registerDataFrameAsTable(dataOffline1,"dataOffline")
    dataOffline2=sqlC.sql("select user_id,Merchant_id,Coupon_id,Discount_rate0,Discount_rate1,Distance, Date_received,Date from dataOffline")
    dataOffline2.repartition(3).write.save('/hadoop/hadoop_/wangjin/ccf_offline_train.parquet')
    #dataOffline=dataOffline.repartition(3)
    #dataOffline.saveAsPickleFile('/hadoop/hadoop_/wangjin/ccf_offline_train')
    
    #offline_test
    dataTest=sc.textFile('/hadoop/hadoop_/wangjin/ccf_offline_test.txt')
    dataTest=dataTest.map(lambda x:x.split(','))
    dataOfflineTest=dataTest.map(lambda x:(x[0],x[1],x[2],process(x[3])[0],process(x[3])[1],x[4],x[5]))
    dataOfflineTestName=Row('user_id','Merchant_id','Coupon_id','Discount_rate0','Discount_rate1','Distance','Date_received')
    dataOfflineTest=dataOfflineTest.map(lambda x:dataOfflineTestName(*x))
    dataOfflineTest1=sqlC.createDataFrame(dataOfflineTest)
    sqlC.registerDataFrameAsTable(dataOfflineTest1,"dataOfflineTest")
    dataOfflineTest2=sqlC.sql("select user_id,Merchant_id,Coupon_id,Discount_rate0,Discount_rate1,Distance, Date_received from dataOfflineTest")
    dataOfflineTest2.repartition(1).write.save('/hadoop/hadoop_/wangjin/ccf_offline_test.parquet')
    #dataOfflineTest=dataOfflineTest.repartition(1)
    #dataOfflineTest.saveAsPickleFile('/hadoop/hadoop_/wangjin/ccf_offline_test')
